"""
Unit tests for the apply_airflow_optimization script.

Tests configuration application, backup, and changelog functionality.
"""

import configparser
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from apply_airflow_optimization import ConfigurationApplier

from common.airflow_optimization_config import AirflowOptimizationConfig


class TestConfigurationApplier:
    """Test suite for ConfigurationApplier class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return AirflowOptimizationConfig(
            scheduler_heartbeat_sec=5, min_file_process_interval=15, parallelism=20
        )

    @pytest.fixture
    def applier(self, config, temp_dir):
        """Create a ConfigurationApplier instance for testing."""
        airflow_cfg_path = temp_dir / "airflow.cfg"
        backup_dir = temp_dir / "backups"

        return ConfigurationApplier(
            config=config, airflow_cfg_path=airflow_cfg_path, backup_dir=backup_dir
        )

    def test_validate_configuration_valid(self, applier):
        """Test validation with valid configuration."""
        is_valid, error_msg = applier.validate_configuration()

        assert is_valid is True
        assert error_msg is None

    def test_validate_configuration_invalid(self, temp_dir):
        """Test validation with invalid configuration."""
        invalid_config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=-1  # Invalid value
        )

        applier = ConfigurationApplier(
            config=invalid_config,
            airflow_cfg_path=temp_dir / "airflow.cfg",
            backup_dir=temp_dir / "backups",
        )

        is_valid, error_msg = applier.validate_configuration()

        assert is_valid is False
        assert error_msg is not None
        assert "scheduler_heartbeat_sec must be >= 1" in error_msg

    def test_backup_current_configuration(self, applier, temp_dir):
        """Test configuration backup functionality."""
        # Create a dummy airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        with open(airflow_cfg_path, "w") as f:
            f.write("[core]\nparallelism = 16\n")

        applier.airflow_cfg_path = airflow_cfg_path

        # Perform backup
        success, backup_path = applier.backup_current_configuration()

        assert success is True
        assert backup_path is not None
        assert backup_path.exists()

        # Verify backup contains airflow.cfg
        backed_up_cfg = backup_path / "airflow.cfg"
        assert backed_up_cfg.exists()

        # Verify backup contains environment vars
        env_backup = backup_path / "environment_vars.txt"
        assert env_backup.exists()

    def test_apply_to_environment_variables(self, applier):
        """Test applying configuration to environment variables."""
        # Clear any existing Airflow env vars
        for key in applier.config.to_env_dict().keys():
            os.environ.pop(key, None)

        # Apply configuration
        success = applier.apply_to_environment_variables()

        assert success is True

        # Verify environment variables are set
        assert os.environ.get("AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC") == "5"
        assert os.environ.get("AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL") == "15"
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "20"

    def test_apply_to_airflow_cfg(self, applier, temp_dir):
        """Test applying configuration to airflow.cfg file."""
        # Create a dummy airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        config_parser = configparser.ConfigParser()
        config_parser.add_section("core")
        config_parser.set("core", "parallelism", "16")

        with open(airflow_cfg_path, "w") as f:
            config_parser.write(f)

        applier.airflow_cfg_path = airflow_cfg_path

        # Apply configuration
        success = applier.apply_to_airflow_cfg()

        assert success is True

        # Verify configuration was updated
        updated_parser = configparser.ConfigParser()
        updated_parser.read(airflow_cfg_path)

        assert updated_parser.get("core", "parallelism") == "20"
        assert updated_parser.get("scheduler", "scheduler_heartbeat_sec") == "5"
        assert updated_parser.get("scheduler", "min_file_process_interval") == "15"

    def test_apply_to_airflow_cfg_nonexistent(self, applier, temp_dir):
        """Test applying configuration when airflow.cfg doesn't exist."""
        # Set path to non-existent file
        applier.airflow_cfg_path = temp_dir / "nonexistent.cfg"

        # Should succeed but skip the file
        success = applier.apply_to_airflow_cfg()

        assert success is True

    def test_changelog_creation(self, applier, temp_dir):
        """Test that changelog file is created and populated."""
        # Create a dummy airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        with open(airflow_cfg_path, "w") as f:
            f.write("[core]\nparallelism = 16\n")

        applier.airflow_cfg_path = airflow_cfg_path

        # Apply configuration
        success, error_msg = applier.apply_all()

        assert success is True
        assert error_msg is None

        # Verify changelog exists
        assert applier.changelog_path.exists()

        # Verify changelog contains entries
        with open(applier.changelog_path) as f:
            content = f.read()
            assert "Configuration Change:" in content
            assert "parallelism" in content or "PARALLELISM" in content

    def test_apply_all_success(self, applier, temp_dir):
        """Test complete application process."""
        # Create a dummy airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        with open(airflow_cfg_path, "w") as f:
            f.write("[core]\nparallelism = 16\n")

        applier.airflow_cfg_path = airflow_cfg_path

        # Apply all
        success, error_msg = applier.apply_all()

        assert success is True
        assert error_msg is None

        # Verify backup was created
        backups = list(applier.backup_dir.glob("airflow_config_backup_*"))
        assert len(backups) > 0

        # Verify changelog was created
        assert applier.changelog_path.exists()

    def test_apply_all_validation_failure(self, temp_dir):
        """Test that apply_all fails when validation fails."""
        invalid_config = AirflowOptimizationConfig(scheduler_heartbeat_sec=-1)

        applier = ConfigurationApplier(
            config=invalid_config,
            airflow_cfg_path=temp_dir / "airflow.cfg",
            backup_dir=temp_dir / "backups",
        )

        success, error_msg = applier.apply_all()

        assert success is False
        assert error_msg is not None
        assert "Validation failed" in error_msg

    def test_apply_section_changes(self, applier):
        """Test applying changes to a configuration section."""
        config_parser = configparser.ConfigParser()
        config_parser.add_section("test")
        config_parser.set("test", "key1", "old_value")

        changes = applier._apply_section_changes(
            config_parser, "test", {"key1": "new_value", "key2": "value2"}
        )

        assert len(changes) == 2
        assert config_parser.get("test", "key1") == "new_value"
        assert config_parser.get("test", "key2") == "value2"

    def test_selective_application(self, applier, temp_dir):
        """Test applying only environment variables or only airflow.cfg."""
        # Create a dummy airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        with open(airflow_cfg_path, "w") as f:
            f.write("[core]\nparallelism = 16\n")

        applier.airflow_cfg_path = airflow_cfg_path

        # Apply only environment variables
        success, error_msg = applier.apply_all(apply_env_vars=True, apply_cfg=False)

        assert success is True
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "20"

        # Verify airflow.cfg was not modified
        config_parser = configparser.ConfigParser()
        config_parser.read(airflow_cfg_path)
        assert config_parser.get("core", "parallelism") == "16"

    def test_list_backups(self, applier, temp_dir):
        """Test listing available backups."""
        # Initially no backups
        backups = applier.list_backups()
        assert len(backups) == 0

        # Create some backup directories
        backup1 = applier.backup_dir / "airflow_config_backup_20240101_120000"
        backup2 = applier.backup_dir / "airflow_config_backup_20240102_120000"
        backup1.mkdir(parents=True)
        backup2.mkdir(parents=True)

        # List backups
        backups = applier.list_backups()
        assert len(backups) == 2

        # Should be sorted newest first
        assert backups[0].name == "airflow_config_backup_20240102_120000"
        assert backups[1].name == "airflow_config_backup_20240101_120000"

    def test_get_latest_backup(self, applier, temp_dir):
        """Test getting the latest backup."""
        # No backups initially
        latest = applier.get_latest_backup()
        assert latest is None

        # Create backups
        backup1 = applier.backup_dir / "airflow_config_backup_20240101_120000"
        backup2 = applier.backup_dir / "airflow_config_backup_20240102_120000"
        backup1.mkdir(parents=True)
        backup2.mkdir(parents=True)

        # Get latest
        latest = applier.get_latest_backup()
        assert latest is not None
        assert latest.name == "airflow_config_backup_20240102_120000"

    def test_verify_backup_valid(self, applier, temp_dir):
        """Test backup verification with valid backup."""
        # Create a valid backup
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        # Create required files
        env_file = backup_path / "environment_vars.txt"
        env_file.write_text("AIRFLOW__CORE__PARALLELISM=16\n")

        cfg_file = backup_path / "airflow.cfg"
        cfg_file.write_text("[core]\nparallelism = 16\n")

        # Verify
        is_valid = applier._verify_backup(backup_path)
        assert is_valid is True

    def test_verify_backup_missing_env_vars(self, applier, temp_dir):
        """Test backup verification with missing environment vars file."""
        # Create backup without env vars file
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        # Verify should fail
        is_valid = applier._verify_backup(backup_path)
        assert is_valid is False

    def test_verify_backup_nonexistent(self, applier, temp_dir):
        """Test backup verification with nonexistent path."""
        backup_path = applier.backup_dir / "nonexistent_backup"

        is_valid = applier._verify_backup(backup_path)
        assert is_valid is False

    def test_restore_environment_variables(self, applier, temp_dir):
        """Test restoring environment variables from backup."""
        # Set some current values
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"
        os.environ["AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC"] = "5"

        # Create backup with different values
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        env_file = backup_path / "environment_vars.txt"
        env_file.write_text(
            "# Backup\n"
            "AIRFLOW__CORE__PARALLELISM=16\n"
            "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=10\n"
        )

        # Restore
        success, changes = applier._restore_environment_variables(backup_path)

        assert success is True
        assert len(changes) == 2
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "16"
        assert os.environ.get("AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC") == "10"

    def test_restore_environment_variables_with_unset(self, applier, temp_dir):
        """Test restoring environment variables that were not set in backup."""
        # Set a current value
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"

        # Create backup where this variable was not set
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        env_file = backup_path / "environment_vars.txt"
        env_file.write_text("# Backup\nAIRFLOW__CORE__PARALLELISM=<not set>\n")

        # Restore
        success, changes = applier._restore_environment_variables(backup_path)

        assert success is True
        assert "AIRFLOW__CORE__PARALLELISM" not in os.environ

    def test_restore_airflow_cfg(self, applier, temp_dir):
        """Test restoring airflow.cfg from backup."""
        # Create current airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        config_parser = configparser.ConfigParser()
        config_parser.add_section("core")
        config_parser.set("core", "parallelism", "20")
        config_parser.add_section("scheduler")
        config_parser.set("scheduler", "scheduler_heartbeat_sec", "5")

        with open(airflow_cfg_path, "w") as f:
            config_parser.write(f)

        applier.airflow_cfg_path = airflow_cfg_path

        # Create backup with different values
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        backup_cfg = backup_path / "airflow.cfg"
        backup_parser = configparser.ConfigParser()
        backup_parser.add_section("core")
        backup_parser.set("core", "parallelism", "16")
        backup_parser.add_section("scheduler")
        backup_parser.set("scheduler", "scheduler_heartbeat_sec", "10")

        with open(backup_cfg, "w") as f:
            backup_parser.write(f)

        # Restore
        success, changes = applier._restore_airflow_cfg(backup_path)

        assert success is True
        assert len(changes) == 2

        # Verify restoration
        restored_parser = configparser.ConfigParser()
        restored_parser.read(airflow_cfg_path)
        assert restored_parser.get("core", "parallelism") == "16"
        assert restored_parser.get("scheduler", "scheduler_heartbeat_sec") == "10"

    def test_restore_airflow_cfg_no_backup(self, applier, temp_dir):
        """Test restoring airflow.cfg when backup doesn't have one."""
        # Create current airflow.cfg
        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 20\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create backup without airflow.cfg
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)

        # Should succeed but not change anything
        success, changes = applier._restore_airflow_cfg(backup_path)

        assert success is True
        assert len(changes) == 0

        # Verify file unchanged
        assert airflow_cfg_path.read_text() == "[core]\nparallelism = 20\n"

    def test_rollback_from_backup_latest(self, applier, temp_dir):
        """Test rolling back from the latest backup."""
        # Set current state
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"

        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 20\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create a backup with old values
        backup_path = applier.backup_dir / "airflow_config_backup_20240101_120000"
        backup_path.mkdir(parents=True)

        env_file = backup_path / "environment_vars.txt"
        env_file.write_text("AIRFLOW__CORE__PARALLELISM=16\n")

        cfg_file = backup_path / "airflow.cfg"
        cfg_file.write_text("[core]\nparallelism = 16\n")

        # Rollback (should use latest backup)
        success, error_msg = applier.rollback_from_backup()

        assert success is True
        assert error_msg is None

        # Verify rollback
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "16"

        restored_parser = configparser.ConfigParser()
        restored_parser.read(airflow_cfg_path)
        assert restored_parser.get("core", "parallelism") == "16"

    def test_rollback_from_backup_specific(self, applier, temp_dir):
        """Test rolling back from a specific backup."""
        # Set current state
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "30"

        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 30\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create multiple backups
        backup1 = applier.backup_dir / "airflow_config_backup_20240101_120000"
        backup1.mkdir(parents=True)
        (backup1 / "environment_vars.txt").write_text("AIRFLOW__CORE__PARALLELISM=16\n")
        (backup1 / "airflow.cfg").write_text("[core]\nparallelism = 16\n")

        backup2 = applier.backup_dir / "airflow_config_backup_20240102_120000"
        backup2.mkdir(parents=True)
        (backup2 / "environment_vars.txt").write_text("AIRFLOW__CORE__PARALLELISM=20\n")
        (backup2 / "airflow.cfg").write_text("[core]\nparallelism = 20\n")

        # Rollback to specific (older) backup
        success, error_msg = applier.rollback_from_backup(backup_path=backup1)

        assert success is True
        assert error_msg is None

        # Verify rollback to backup1 values
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "16"

        restored_parser = configparser.ConfigParser()
        restored_parser.read(airflow_cfg_path)
        assert restored_parser.get("core", "parallelism") == "16"

    def test_rollback_no_backups(self, applier):
        """Test rollback when no backups exist."""
        success, error_msg = applier.rollback_from_backup()

        assert success is False
        assert error_msg is not None
        assert "No backups found" in error_msg

    def test_rollback_invalid_backup(self, applier, temp_dir):
        """Test rollback with invalid backup path."""
        backup_path = applier.backup_dir / "nonexistent_backup"

        success, error_msg = applier.rollback_from_backup(backup_path=backup_path)

        assert success is False
        assert error_msg is not None
        assert "does not exist" in error_msg

    def test_rollback_incomplete_backup(self, applier, temp_dir):
        """Test rollback with incomplete backup (missing env vars file)."""
        # Create incomplete backup
        backup_path = applier.backup_dir / "incomplete_backup"
        backup_path.mkdir(parents=True)
        # Missing environment_vars.txt

        success, error_msg = applier.rollback_from_backup(backup_path=backup_path)

        assert success is False
        assert error_msg is not None
        assert "invalid or incomplete" in error_msg

    def test_rollback_selective(self, applier, temp_dir):
        """Test selective rollback (only env vars or only cfg)."""
        # Set current state
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"

        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 20\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create backup
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)
        (backup_path / "environment_vars.txt").write_text("AIRFLOW__CORE__PARALLELISM=16\n")
        (backup_path / "airflow.cfg").write_text("[core]\nparallelism = 16\n")

        # Rollback only environment variables
        success, error_msg = applier.rollback_from_backup(
            backup_path=backup_path, restore_env_vars=True, restore_cfg=False
        )

        assert success is True

        # Verify only env vars were restored
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "16"

        # airflow.cfg should remain unchanged
        assert airflow_cfg_path.read_text() == "[core]\nparallelism = 20\n"

    def test_rollback_creates_pre_rollback_backup(self, applier, temp_dir):
        """Test that rollback creates a backup of current state before rolling back."""
        # Set current state
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"

        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 20\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create backup to rollback to
        backup_path = applier.backup_dir / "airflow_config_backup_20240101_120000"
        backup_path.mkdir(parents=True)
        (backup_path / "environment_vars.txt").write_text("AIRFLOW__CORE__PARALLELISM=16\n")
        (backup_path / "airflow.cfg").write_text("[core]\nparallelism = 16\n")

        # Count backups before rollback
        backups_before = len(applier.list_backups())

        # Rollback
        success, error_msg = applier.rollback_from_backup()

        assert success is True

        # Should have created a new backup (pre-rollback backup)
        backups_after = len(applier.list_backups())
        assert backups_after == backups_before + 1

    def test_rollback_logs_to_changelog(self, applier, temp_dir):
        """Test that rollback logs changes to changelog."""
        # Set current state
        os.environ["AIRFLOW__CORE__PARALLELISM"] = "20"

        airflow_cfg_path = temp_dir / "airflow.cfg"
        airflow_cfg_path.write_text("[core]\nparallelism = 20\n")
        applier.airflow_cfg_path = airflow_cfg_path

        # Create backup
        backup_path = applier.backup_dir / "test_backup"
        backup_path.mkdir(parents=True)
        (backup_path / "environment_vars.txt").write_text("AIRFLOW__CORE__PARALLELISM=16\n")
        (backup_path / "airflow.cfg").write_text("[core]\nparallelism = 16\n")

        # Rollback
        success, error_msg = applier.rollback_from_backup(backup_path=backup_path)

        assert success is True

        # Verify changelog was updated
        assert applier.changelog_path.exists()

        changelog_content = applier.changelog_path.read_text()
        assert "Configuration Rollback:" in changelog_content
        assert str(backup_path) in changelog_content

    def test_apply_and_rollback_restores_exact_state(self, applier, temp_dir):
        """
        Integration test: Apply configuration, then rollback should restore exact previous state.
        This validates Requirement 8.4.
        """
        # Set initial state
        initial_parallelism = "16"
        initial_heartbeat = "10"

        os.environ["AIRFLOW__CORE__PARALLELISM"] = initial_parallelism
        os.environ["AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC"] = initial_heartbeat

        airflow_cfg_path = temp_dir / "airflow.cfg"
        initial_cfg = configparser.ConfigParser()
        initial_cfg.add_section("core")
        initial_cfg.set("core", "parallelism", initial_parallelism)
        initial_cfg.add_section("scheduler")
        initial_cfg.set("scheduler", "scheduler_heartbeat_sec", initial_heartbeat)

        with open(airflow_cfg_path, "w") as f:
            initial_cfg.write(f)

        applier.airflow_cfg_path = airflow_cfg_path

        # Apply new configuration (this creates a backup of initial state)
        success, error_msg = applier.apply_all()
        assert success is True

        # Get the backup that was just created (should be the only one)
        backups = applier.list_backups()
        assert len(backups) == 1
        initial_backup = backups[0]

        # Verify the backup contains the initial values
        backup_env_file = initial_backup / "environment_vars.txt"
        backup_env_content = backup_env_file.read_text()
        assert f"AIRFLOW__CORE__PARALLELISM={initial_parallelism}" in backup_env_content

        # Verify new configuration was applied
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == "20"
        assert os.environ.get("AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC") == "5"

        new_cfg = configparser.ConfigParser()
        new_cfg.read(airflow_cfg_path)
        assert new_cfg.get("core", "parallelism") == "20"
        assert new_cfg.get("scheduler", "scheduler_heartbeat_sec") == "5"

        # Now rollback to the specific backup that contains the initial state
        success, error_msg = applier.rollback_from_backup(backup_path=initial_backup)
        assert success is True

        # Verify exact restoration of initial state
        assert os.environ.get("AIRFLOW__CORE__PARALLELISM") == initial_parallelism, (
            f"Expected {initial_parallelism}, got {os.environ.get('AIRFLOW__CORE__PARALLELISM')}"
        )
        assert os.environ.get("AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC") == initial_heartbeat

        restored_cfg = configparser.ConfigParser()
        restored_cfg.read(airflow_cfg_path)
        assert restored_cfg.get("core", "parallelism") == initial_parallelism
        assert restored_cfg.get("scheduler", "scheduler_heartbeat_sec") == initial_heartbeat


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
