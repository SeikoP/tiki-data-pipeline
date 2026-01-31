#!/usr/bin/env python3
"""
Configuration application script for Airflow task startup optimization.

This script applies optimization configuration to Apache Airflow by:
1. Validating the configuration
2. Backing up current configuration
3. Applying new configuration to environment variables and/or airflow.cfg
4. Logging all changes to a changelog file

It also supports rolling back to previous configurations.

Usage:
    # Apply optimization configuration
    python apply_airflow_optimization.py apply

    # Rollback to the latest backup
    python apply_airflow_optimization.py rollback

    # Rollback to a specific backup
    python apply_airflow_optimization.py rollback --backup-path /path/to/backup

    # List available backups
    python apply_airflow_optimization.py list-backups

    # Apply only environment variables (skip airflow.cfg)
    python apply_airflow_optimization.py apply --no-cfg

    # Rollback only airflow.cfg (skip environment variables)
    python apply_airflow_optimization.py rollback --no-env-vars

Requirements: 8.3, 8.4, 8.5
"""

import os
import sys
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import configparser

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from common.airflow_optimization_config import AirflowOptimizationConfig


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConfigurationApplier:
    """Applies Airflow optimization configuration with backup and logging."""
    
    def __init__(
        self,
        config: AirflowOptimizationConfig,
        airflow_cfg_path: Optional[Path] = None,
        backup_dir: Optional[Path] = None,
        changelog_path: Optional[Path] = None
    ):
        """
        Initialize the configuration applier.
        
        Args:
            config: The optimization configuration to apply
            airflow_cfg_path: Path to airflow.cfg file (optional)
            backup_dir: Directory for configuration backups
            changelog_path: Path to changelog file
        """
        self.config = config
        self.airflow_cfg_path = airflow_cfg_path or Path('/opt/airflow/airflow.cfg')
        self.backup_dir = backup_dir or Path(__file__).parent.parent / 'backups'
        self.changelog_path = changelog_path or self.backup_dir / 'config_changelog.log'
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_configuration(self) -> tuple[bool, Optional[str]]:
        """
        Validate the configuration before applying.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        logger.info("Validating configuration...")
        is_valid, error_msg = self.config.validate()
        
        if not is_valid:
            logger.error(f"Configuration validation failed: {error_msg}")
        else:
            logger.info("Configuration validation passed")
        
        return is_valid, error_msg
    
    def backup_current_configuration(self) -> tuple[bool, Optional[Path]]:
        """
        Backup current configuration files before applying changes.
        
        Returns:
            Tuple of (success, backup_path)
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')  # Added microseconds
        backup_path = self.backup_dir / f'airflow_config_backup_{timestamp}'
        
        try:
            backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Creating configuration backup at {backup_path}")
            
            # Backup airflow.cfg if it exists
            if self.airflow_cfg_path.exists():
                cfg_backup = backup_path / 'airflow.cfg'
                shutil.copy2(self.airflow_cfg_path, cfg_backup)
                logger.info(f"Backed up airflow.cfg to {cfg_backup}")
            else:
                logger.warning(f"airflow.cfg not found at {self.airflow_cfg_path}")
            
            # Backup current environment variables
            env_backup = backup_path / 'environment_vars.txt'
            with open(env_backup, 'w') as f:
                f.write("# Current Airflow environment variables\n")
                f.write(f"# Backup created: {datetime.now().isoformat()}\n\n")
                
                for key in self.config.to_env_dict().keys():
                    value = os.environ.get(key, '<not set>')
                    f.write(f"{key}={value}\n")
            
            logger.info(f"Backed up environment variables to {env_backup}")
            logger.info(f"Configuration backup completed successfully")
            
            return True, backup_path
            
        except Exception as e:
            logger.error(f"Failed to backup configuration: {e}", exc_info=True)
            return False, None
    
    def apply_to_environment_variables(self) -> bool:
        """
        Apply configuration to environment variables.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Applying configuration to environment variables...")
            env_dict = self.config.to_env_dict()
            
            changes = []
            for key, value in env_dict.items():
                old_value = os.environ.get(key, '<not set>')
                os.environ[key] = value
                
                if old_value != value:
                    changes.append(f"{key}: {old_value} -> {value}")
                    logger.info(f"Set {key}={value}")
            
            if changes:
                self._log_changes_to_changelog(
                    "Environment Variables",
                    changes
                )
            
            logger.info("Environment variables applied successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply environment variables: {e}", exc_info=True)
            return False
    
    def apply_to_airflow_cfg(self) -> bool:
        """
        Apply configuration to airflow.cfg file.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.airflow_cfg_path.exists():
            logger.warning(f"airflow.cfg not found at {self.airflow_cfg_path}, skipping")
            return True
        
        try:
            logger.info(f"Applying configuration to {self.airflow_cfg_path}...")
            
            # Read existing configuration
            config_parser = configparser.ConfigParser()
            config_parser.read(self.airflow_cfg_path)
            
            changes = []
            
            # Apply scheduler settings
            if 'scheduler' not in config_parser:
                config_parser.add_section('scheduler')
            
            scheduler_changes = self._apply_section_changes(
                config_parser,
                'scheduler',
                {
                    'scheduler_heartbeat_sec': str(self.config.scheduler_heartbeat_sec),
                    'min_file_process_interval': str(self.config.min_file_process_interval),
                    'dag_dir_list_interval': str(self.config.dag_dir_list_interval),
                    'parsing_processes': str(self.config.parsing_processes),
                }
            )
            changes.extend(scheduler_changes)
            
            # Apply database settings
            if 'database' not in config_parser:
                config_parser.add_section('database')
            
            database_changes = self._apply_section_changes(
                config_parser,
                'database',
                {
                    'sql_alchemy_pool_size': str(self.config.sql_alchemy_pool_size),
                    'sql_alchemy_max_overflow': str(self.config.sql_alchemy_max_overflow),
                    'sql_alchemy_pool_recycle': str(self.config.sql_alchemy_pool_recycle),
                    'sql_alchemy_pool_timeout': str(self.config.sql_alchemy_pool_timeout),
                }
            )
            changes.extend(database_changes)
            
            # Apply core settings
            if 'core' not in config_parser:
                config_parser.add_section('core')
            
            core_changes = self._apply_section_changes(
                config_parser,
                'core',
                {
                    'parallelism': str(self.config.parallelism),
                    'max_active_tasks_per_dag': str(self.config.max_active_tasks_per_dag),
                    'max_active_runs_per_dag': str(self.config.max_active_runs_per_dag),
                    'store_serialized_dags': str(self.config.store_serialized_dags),
                    'store_dag_code': str(self.config.store_dag_code),
                    'min_serialized_dag_update_interval': str(self.config.min_serialized_dag_update_interval),
                }
            )
            changes.extend(core_changes)
            
            # Write updated configuration
            with open(self.airflow_cfg_path, 'w') as f:
                config_parser.write(f)
            
            if changes:
                self._log_changes_to_changelog(
                    f"airflow.cfg ({self.airflow_cfg_path})",
                    changes
                )
            
            logger.info(f"airflow.cfg updated successfully with {len(changes)} changes")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply configuration to airflow.cfg: {e}", exc_info=True)
            return False
    
    def _apply_section_changes(
        self,
        config_parser: configparser.ConfigParser,
        section: str,
        settings: dict
    ) -> list[str]:
        """
        Apply settings to a configuration section and track changes.
        
        Args:
            config_parser: The ConfigParser instance
            section: The section name
            settings: Dictionary of setting name to value
        
        Returns:
            List of change descriptions
        """
        changes = []
        
        for key, value in settings.items():
            old_value = config_parser.get(section, key, fallback='<not set>')
            
            if old_value != value:
                config_parser.set(section, key, value)
                changes.append(f"[{section}] {key}: {old_value} -> {value}")
                logger.info(f"Updated [{section}] {key}={value}")
        
        return changes
    
    def _log_changes_to_changelog(self, target: str, changes: list[str]) -> None:
        """
        Log configuration changes to the changelog file.
        
        Args:
            target: The target being changed (e.g., "Environment Variables")
            changes: List of change descriptions
        """
        try:
            timestamp = datetime.now().isoformat()
            
            with open(self.changelog_path, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Configuration Change: {timestamp}\n")
                f.write(f"Target: {target}\n")
                f.write(f"{'='*80}\n\n")
                
                for change in changes:
                    f.write(f"  - {change}\n")
                
                f.write("\n")
            
            logger.info(f"Logged {len(changes)} changes to {self.changelog_path}")
            
        except Exception as e:
            logger.error(f"Failed to log changes to changelog: {e}", exc_info=True)
    
    def apply_all(
        self,
        apply_env_vars: bool = True,
        apply_cfg: bool = True
    ) -> tuple[bool, Optional[str]]:
        """
        Apply configuration with validation and backup.
        
        Args:
            apply_env_vars: Whether to apply to environment variables
            apply_cfg: Whether to apply to airflow.cfg
        
        Returns:
            Tuple of (success, error_message)
        """
        logger.info("Starting configuration application process...")
        
        # Step 1: Validate configuration
        is_valid, error_msg = self.validate_configuration()
        if not is_valid:
            return False, f"Validation failed: {error_msg}"
        
        # Step 2: Backup current configuration
        backup_success, backup_path = self.backup_current_configuration()
        if not backup_success:
            return False, "Failed to backup current configuration"
        
        logger.info(f"Configuration backed up to: {backup_path}")
        
        # Step 3: Apply configuration
        success = True
        errors = []
        
        if apply_env_vars:
            if not self.apply_to_environment_variables():
                success = False
                errors.append("Failed to apply environment variables")
        
        if apply_cfg:
            if not self.apply_to_airflow_cfg():
                success = False
                errors.append("Failed to apply airflow.cfg changes")
        
        if not success:
            error_msg = "; ".join(errors)
            logger.error(f"Configuration application failed: {error_msg}")
            logger.info(f"You can restore from backup at: {backup_path}")
            return False, error_msg
        
        logger.info("Configuration applied successfully!")
        logger.info(f"Backup available at: {backup_path}")
        logger.info(f"Changelog updated at: {self.changelog_path}")
        
        return True, None
    
    def list_backups(self) -> list[Path]:
        """
        List all available configuration backups.
        
        Returns:
            List of backup directory paths, sorted by timestamp (newest first)
        """
        if not self.backup_dir.exists():
            logger.warning(f"Backup directory does not exist: {self.backup_dir}")
            return []
        
        backups = [
            d for d in self.backup_dir.iterdir()
            if d.is_dir() and d.name.startswith('airflow_config_backup_')
        ]
        
        # Sort by timestamp in directory name (newest first)
        backups.sort(reverse=True)
        
        return backups
    
    def get_latest_backup(self) -> Optional[Path]:
        """
        Get the most recent configuration backup.
        
        Returns:
            Path to the latest backup directory, or None if no backups exist
        """
        backups = self.list_backups()
        return backups[0] if backups else None
    
    def rollback_from_backup(
        self,
        backup_path: Optional[Path] = None,
        restore_env_vars: bool = True,
        restore_cfg: bool = True
    ) -> tuple[bool, Optional[str]]:
        """
        Rollback configuration from a backup directory.
        
        Args:
            backup_path: Path to backup directory (uses latest if None)
            restore_env_vars: Whether to restore environment variables
            restore_cfg: Whether to restore airflow.cfg
        
        Returns:
            Tuple of (success, error_message)
        """
        logger.info("Starting configuration rollback process...")
        
        # Determine which backup to use
        if backup_path is None:
            backup_path = self.get_latest_backup()
            if backup_path is None:
                error_msg = "No backups found to rollback from"
                logger.error(error_msg)
                return False, error_msg
            logger.info(f"Using latest backup: {backup_path}")
        else:
            if not backup_path.exists():
                error_msg = f"Backup path does not exist: {backup_path}"
                logger.error(error_msg)
                return False, error_msg
            logger.info(f"Using specified backup: {backup_path}")
        
        # Verify backup contains expected files
        if not self._verify_backup(backup_path):
            error_msg = f"Backup directory is invalid or incomplete: {backup_path}"
            logger.error(error_msg)
            return False, error_msg
        
        # Create a backup of current state before rollback
        logger.info("Creating backup of current state before rollback...")
        pre_rollback_success, pre_rollback_path = self.backup_current_configuration()
        if not pre_rollback_success:
            logger.warning("Failed to backup current state before rollback, continuing anyway...")
        else:
            logger.info(f"Pre-rollback backup created at: {pre_rollback_path}")
        
        # Perform rollback
        success = True
        errors = []
        changes = []
        
        if restore_env_vars:
            env_success, env_changes = self._restore_environment_variables(backup_path)
            if not env_success:
                success = False
                errors.append("Failed to restore environment variables")
            else:
                changes.extend(env_changes)
        
        if restore_cfg:
            cfg_success, cfg_changes = self._restore_airflow_cfg(backup_path)
            if not cfg_success:
                success = False
                errors.append("Failed to restore airflow.cfg")
            else:
                changes.extend(cfg_changes)
        
        if not success:
            error_msg = "; ".join(errors)
            logger.error(f"Configuration rollback failed: {error_msg}")
            if pre_rollback_path:
                logger.info(f"Current state backed up at: {pre_rollback_path}")
            return False, error_msg
        
        # Log rollback to changelog
        if changes:
            self._log_rollback_to_changelog(backup_path, changes)
        
        logger.info("Configuration rollback completed successfully!")
        logger.info(f"Restored from backup: {backup_path}")
        if pre_rollback_path:
            logger.info(f"Pre-rollback backup available at: {pre_rollback_path}")
        
        return True, None
    
    def _verify_backup(self, backup_path: Path) -> bool:
        """
        Verify that a backup directory contains expected files.
        
        Args:
            backup_path: Path to backup directory
        
        Returns:
            True if backup is valid, False otherwise
        """
        # Check if backup directory exists
        if not backup_path.exists() or not backup_path.is_dir():
            logger.error(f"Backup path is not a valid directory: {backup_path}")
            return False
        
        # Check for environment variables backup
        env_backup = backup_path / 'environment_vars.txt'
        if not env_backup.exists():
            logger.error(f"Environment variables backup not found: {env_backup}")
            return False
        
        # airflow.cfg is optional (may not exist in all environments)
        cfg_backup = backup_path / 'airflow.cfg'
        if cfg_backup.exists():
            logger.info(f"Found airflow.cfg backup: {cfg_backup}")
        else:
            logger.info("No airflow.cfg backup found (this is OK if not using airflow.cfg)")
        
        return True
    
    def _restore_environment_variables(
        self,
        backup_path: Path
    ) -> tuple[bool, list[str]]:
        """
        Restore environment variables from backup.
        
        Args:
            backup_path: Path to backup directory
        
        Returns:
            Tuple of (success, list of changes)
        """
        env_backup = backup_path / 'environment_vars.txt'
        
        try:
            logger.info(f"Restoring environment variables from {env_backup}...")
            
            changes = []
            
            # Read backed up environment variables
            with open(env_backup, 'r') as f:
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value
                    if '=' not in line:
                        continue
                    
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Get current value
                    old_value = os.environ.get(key, '<not set>')
                    
                    # Restore the value
                    if value == '<not set>':
                        # Variable was not set in backup, remove it if currently set
                        if key in os.environ:
                            del os.environ[key]
                            changes.append(f"{key}: {old_value} -> <removed>")
                            logger.info(f"Removed {key}")
                    else:
                        # Set the backed up value
                        if old_value != value:
                            os.environ[key] = value
                            changes.append(f"{key}: {old_value} -> {value}")
                            logger.info(f"Restored {key}={value}")
            
            logger.info(f"Environment variables restored successfully ({len(changes)} changes)")
            return True, changes
            
        except Exception as e:
            logger.error(f"Failed to restore environment variables: {e}", exc_info=True)
            return False, []
    
    def _restore_airflow_cfg(
        self,
        backup_path: Path
    ) -> tuple[bool, list[str]]:
        """
        Restore airflow.cfg from backup.
        
        Args:
            backup_path: Path to backup directory
        
        Returns:
            Tuple of (success, list of changes)
        """
        cfg_backup = backup_path / 'airflow.cfg'
        
        # If backup doesn't have airflow.cfg, skip restoration
        if not cfg_backup.exists():
            logger.info("No airflow.cfg in backup, skipping restoration")
            return True, []
        
        # If target doesn't exist, we can't restore (would be creating new file)
        if not self.airflow_cfg_path.exists():
            logger.warning(f"Target airflow.cfg does not exist at {self.airflow_cfg_path}, skipping restoration")
            return True, []
        
        try:
            logger.info(f"Restoring airflow.cfg from {cfg_backup} to {self.airflow_cfg_path}...")
            
            # Read both configs to track changes
            old_config = configparser.ConfigParser()
            old_config.read(self.airflow_cfg_path)
            
            backup_config = configparser.ConfigParser()
            backup_config.read(cfg_backup)
            
            changes = []
            
            # Track all sections and keys that changed
            all_sections = set(old_config.sections()) | set(backup_config.sections())
            
            for section in all_sections:
                if section in backup_config.sections():
                    for key in backup_config.options(section):
                        old_value = old_config.get(section, key, fallback='<not set>')
                        new_value = backup_config.get(section, key)
                        
                        if old_value != new_value:
                            changes.append(f"[{section}] {key}: {old_value} -> {new_value}")
            
            # Copy the backup file to restore configuration
            shutil.copy2(cfg_backup, self.airflow_cfg_path)
            
            logger.info(f"airflow.cfg restored successfully ({len(changes)} changes)")
            return True, changes
            
        except Exception as e:
            logger.error(f"Failed to restore airflow.cfg: {e}", exc_info=True)
            return False, []
    
    def _log_rollback_to_changelog(
        self,
        backup_path: Path,
        changes: list[str]
    ) -> None:
        """
        Log rollback operation to the changelog file.
        
        Args:
            backup_path: Path to the backup that was restored
            changes: List of change descriptions
        """
        try:
            timestamp = datetime.now().isoformat()
            
            with open(self.changelog_path, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Configuration Rollback: {timestamp}\n")
                f.write(f"Restored from backup: {backup_path}\n")
                f.write(f"{'='*80}\n\n")
                
                for change in changes:
                    f.write(f"  - {change}\n")
                
                f.write("\n")
            
            logger.info(f"Logged rollback with {len(changes)} changes to {self.changelog_path}")
            
        except Exception as e:
            logger.error(f"Failed to log rollback to changelog: {e}", exc_info=True)


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Apply or rollback Airflow optimization configuration'
    )
    parser.add_argument(
        'action',
        choices=['apply', 'rollback', 'list-backups'],
        help='Action to perform: apply new config, rollback to backup, or list available backups'
    )
    parser.add_argument(
        '--backup-path',
        type=str,
        help='Specific backup directory to rollback from (uses latest if not specified)'
    )
    parser.add_argument(
        '--no-env-vars',
        action='store_true',
        help='Skip environment variables (apply/rollback only airflow.cfg)'
    )
    parser.add_argument(
        '--no-cfg',
        action='store_true',
        help='Skip airflow.cfg (apply/rollback only environment variables)'
    )
    
    args = parser.parse_args()
    
    # Create default optimization configuration
    config = AirflowOptimizationConfig()
    
    # Determine airflow.cfg path
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    airflow_cfg_path = Path(airflow_home) / 'airflow.cfg'
    
    # Alternative path for local development
    if not airflow_cfg_path.exists():
        airflow_cfg_path = Path(__file__).parent.parent / 'airflow' / 'config' / 'airflow.cfg'
    
    # Create applier
    applier = ConfigurationApplier(
        config=config,
        airflow_cfg_path=airflow_cfg_path
    )
    
    # Handle different actions
    if args.action == 'list-backups':
        backups = applier.list_backups()
        if not backups:
            logger.info("No backups found")
            sys.exit(0)
        
        logger.info(f"Found {len(backups)} backup(s):")
        for i, backup in enumerate(backups, 1):
            logger.info(f"  {i}. {backup.name} ({backup})")
        
        latest = applier.get_latest_backup()
        if latest:
            logger.info(f"\nLatest backup: {latest}")
        
        sys.exit(0)
    
    elif args.action == 'apply':
        # Apply configuration
        success, error_msg = applier.apply_all(
            apply_env_vars=not args.no_env_vars,
            apply_cfg=not args.no_cfg
        )
        
        if success:
            logger.info("✓ Configuration applied successfully")
            sys.exit(0)
        else:
            logger.error(f"✗ Configuration application failed: {error_msg}")
            sys.exit(1)
    
    elif args.action == 'rollback':
        # Rollback configuration
        backup_path = Path(args.backup_path) if args.backup_path else None
        
        success, error_msg = applier.rollback_from_backup(
            backup_path=backup_path,
            restore_env_vars=not args.no_env_vars,
            restore_cfg=not args.no_cfg
        )
        
        if success:
            logger.info("✓ Configuration rollback completed successfully")
            sys.exit(0)
        else:
            logger.error(f"✗ Configuration rollback failed: {error_msg}")
            sys.exit(1)


if __name__ == '__main__':
    main()
