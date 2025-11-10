<!-- SEO -->
<!-- Keywords: Data Pipeline, Airflow, Firecrawl, Docker, Data Engineering, ETL, Web Scraping, Self-Hosted -->

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,0A192F,172A45,64FFDA&height=200&section=header&text=Data%20Pipeline%20Template&fontSize=60&fontColor=fff&animation=twinkling&fontAlignY=35&desc=Apache%20Airflow%20%2B%20Firecrawl%20Self-Hosted&descAlignY=55&descAlign=50"/>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Version-1.0.0-blue?style=for-the-badge&logo=github&logoColor=white"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge&logo=opensourceinitiative&logoColor=white"/>
  <img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge&logo=checkmarx&logoColor=white"/>
  <img src="https://img.shields.io/badge/Template-Ready-orange?style=for-the-badge&logo=template&logoColor=white"/>
</p>

<p align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&size=24&duration=3000&pause=1000&color=64FFDA&center=true&vCenter=true&width=700&lines=🚀+Production-Ready+Template;⚡+Airflow+%2B+Firecrawl+Integration;🐳+Docker+Compose+Optimized;📊+Shared+Databases+Architecture" alt="Typing SVG" />
</p>

---

## ✨ Tính năng nổi bật

<div align="center">

| 🎯 Feature | 📝 Description |
|:---------:|:-------------|
| 🔄 **Apache Airflow 3.1.2** | Workflow orchestration với Celery executor |
| 🕷️ **Firecrawl Self-Host** | Web scraping và crawling engine |
| 🗄️ **Shared Databases** | Tối ưu tài nguyên với 1 Redis + 1 Postgres |
| 🐳 **Docker Compose** | One-command deployment |
| ⚡ **Resource Limits** | Quản lý tài nguyên hiệu quả |
| 🏥 **Health Checks** | Tự động monitoring và recovery |
| 📚 **Full Documentation** | Hướng dẫn chi tiết từ A-Z |
| 🎨 **Template Ready** | Sẵn sàng sử dụng cho dự án mới |

</div>

---

## 🛠️ Tech Stack

<p align="center">
  <img src="https://skillicons.dev/icons?i=docker,kubernetes,postgres,redis,python,airflow,git,github&theme=dark&perline=8"/>
</p>

<div align="center">
  
| Category | Technologies |
|:--------:|:-----------:|
| **Orchestration** | Apache Airflow 3.1.2, Celery |
| **Scraping** | Firecrawl, Playwright |
| **Databases** | PostgreSQL 16, Redis 7.2 |
| **Containerization** | Docker, Docker Compose |
| **Languages** | Python, TypeScript, Node.js |
| **Tools** | Git, GitHub Actions |

</div>

---

## 🚀 Quick Start

### Prerequisites

```bash
✅ Docker >= 20.10
✅ Docker Compose >= 2.0
✅ RAM: 4GB+ (8GB recommended)
✅ CPU: 2+ cores
✅ Disk: 10GB+ free space
```

### Installation

<details>
<summary><b>📋 Click để xem hướng dẫn chi tiết</b></summary>

#### 1. Clone Repository

```bash
# Sử dụng như template
gh repo create my-project --template your-username/tiki-data-pipeline

# Hoặc clone trực tiếp
git clone https://github.com/your-username/tiki-data-pipeline.git
cd tiki-data-pipeline
```

#### 2. Cấu hình môi trường

```bash
# Copy file mẫu
cp .env.example .env

# Chỉnh sửa các biến môi trường
nano .env  # hoặc dùng editor khác
```

#### 3. Khởi động services

```bash
# Build và khởi động
docker-compose up -d

# Xem logs
docker-compose logs -f

# Kiểm tra trạng thái
docker-compose ps
```

#### 4. Truy cập services

- **Airflow Web UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
  
- **Firecrawl API**: http://localhost:3002
  - Docs: http://localhost:3002/docs

</details>

---

## 📊 Architecture

<div align="center">

```mermaid
graph TB
    subgraph "Airflow Services"
        A[Airflow Scheduler]
        B[Airflow API Server]
        C[Airflow Worker]
        D[Airflow Triggerer]
        E[DAG Processor]
    end
    
    subgraph "Firecrawl Services"
        F[Firecrawl API]
        G[Playwright Service]
    end
    
    subgraph "Databases"
        H[(PostgreSQL)]
        I[(Redis)]
    end
    
    A --> H
    A --> I
    B --> H
    C --> I
    C --> H
    F --> I
    F --> H
    F --> G
    
    style H fill:#336791
    style I fill:#DC382D
    style A fill:#017CEE
    style F fill:#FF6B35
```

</div>

### Database Architecture

| Service | Database | Purpose |
|:-------:|:--------:|:-------|
| **Airflow** | PostgreSQL `airflow` | Metadata, DAGs, Task states |
| **Airflow** | Redis DB `0` | Celery message broker |
| **Firecrawl** | PostgreSQL `nuq` | NUQ database |
| **Firecrawl** | Redis DB `1` | Queue & rate limiting |

---

## 📁 Project Structure

```
tiki-data-pipeline/
├── 📄 README.md                 # File này
├── 📄 LICENSE                  # MIT License
├── 📄 .env.example             # Environment variables template
├── 🐳 docker-compose.yaml      # Main configuration
├── 📚 docs/                    # Documentation
│   ├── README.md              # Documentation index
│   ├── QUICK_START.md         # Quick start guide
│   ├── TEMPLATE.md            # Template usage
│   ├── SETUP_GITHUB.md        # GitHub setup
│   └── CONTRIBUTING.md        # Contributing guide
├── 🔧 scripts/                 # Utility scripts
│   ├── init-multiple-databases.sh
│   └── setup-new-project.sh
├── ☁️ airflow/                  # Airflow configuration
│   ├── dags/                  # Your DAGs here
│   ├── logs/                  # Airflow logs
│   ├── config/                # Airflow config
│   └── plugins/               # Airflow plugins
├── 🕷️ firecrawl/               # Firecrawl source
└── 💻 src/                     # Your source code
    ├── pipelines/             # Data pipelines
    ├── models/                # Data models
    └── utils/                 # Utilities
```

---

## 📚 Documentation

<div align="center">

| 📖 Document | 📝 Description | 🔗 Link |
|:----------:|:-------------:|:------:|
| **Quick Start** | Hướng dẫn nhanh để bắt đầu | [📄 docs/QUICK_START.md](docs/QUICK_START.md) |
| **Template Guide** | Cách sử dụng như template | [📄 docs/TEMPLATE.md](docs/TEMPLATE.md) |
| **GitHub Setup** | Setup template repository | [📄 docs/SETUP_GITHUB.md](docs/SETUP_GITHUB.md) |
| **Contributing** | Hướng dẫn contribute | [📄 docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) |
| **Full Docs** | Tổng quan tài liệu | [📄 docs/README.md](docs/README.md) |

</div>

---

## 🎯 Use Cases

<div align="center">

| Use Case | Description | Example |
|:--------:|:-----------|:--------|
| 📊 **ETL Pipelines** | Extract, Transform, Load data | Data warehouse ingestion |
| 🕷️ **Web Scraping** | Automated web data collection | Price monitoring, News aggregation |
| 📈 **Data Processing** | Batch và streaming processing | Analytics, Reporting |
| 🔄 **Workflow Automation** | Complex task orchestration | Multi-step data pipelines |
| 📱 **API Integration** | Connect multiple services | Third-party data sync |

</div>

---

## 🏆 Best Practices

<div align="center">

✅ **Resource Management** - Tất cả services có resource limits  
✅ **Health Monitoring** - Automatic health checks cho tất cả services  
✅ **Security** - Environment variables cho sensitive data  
✅ **Scalability** - Dễ dàng scale từng service độc lập  
✅ **Documentation** - Comprehensive docs cho mọi use case  
✅ **Template Ready** - One-click setup cho dự án mới  

</div>

---

## 📈 Performance & Resources

<div align="center">

| Component | CPU Limit | Memory Limit | Status |
|:---------:|:---------:|:------------:|:------:|
| **PostgreSQL** | 1 core | 1GB | ✅ Optimized |
| **Redis** | 0.5 core | 512MB | ✅ Optimized |
| **Airflow Services** | 0.5-2 cores | 256MB-2GB | ✅ Optimized |
| **Firecrawl Services** | 0.5-2 cores | 512MB-2GB | ✅ Optimized |

**Total Estimated**: ~4-6 CPU cores, ~6-8GB RAM

</div>

---

## 🤝 Contributing

<div align="center">

Chúng tôi hoan nghênh mọi đóng góp! 🎉

[📖 Contributing Guidelines](docs/CONTRIBUTING.md) | [🐛 Report Bug](https://github.com/your-username/tiki-data-pipeline/issues) | [💡 Request Feature](https://github.com/your-username/tiki-data-pipeline/issues)

</div>

---

## 📊 Project Stats

<div align="center">

<p align="center">
  <img src="https://github-readme-stats.vercel.app/api?username=your-username&show_icons=true&theme=github_dark&hide_border=true&title_color=64FFDA&icon_color=64FFDA&text_color=c9d1d9&bg_color=0A192F" width="47%"/>
  <img src="https://github-readme-stats.vercel.app/api/top-langs/?username=your-username&layout=compact&hide_border=true&theme=github_dark&title_color=64FFDA&text_color=c9d1d9&langs_count=8&card_width=420&bg_color=0A192F" width="47%"/>
</p>

<p align="center">
  <img width="100%" src="https://github-readme-activity-graph.vercel.app/graph?username=your-username&custom_title=Contribution%20Graph&bg_color=0A192F&color=64FFDA&line=64FFDA&point=FFFFFF&area_color=64FFDA30&title_color=64FFDA&area=true&hide_border=true&radius=16" alt="Contribution Graph"/>
</p>

</div>

---

## 🔗 Links & Resources

<div align="center">

| Resource | Link |
|:--------:|:----|
| **Apache Airflow** | [Documentation](https://airflow.apache.org/docs/) |
| **Firecrawl** | [Self-Host Guide](https://docs.firecrawl.dev/self-hosting) |
| **Docker Compose** | [Documentation](https://docs.docker.com/compose/) |
| **Issues** | [GitHub Issues](https://github.com/your-username/tiki-data-pipeline/issues) |
| **Discussions** | [GitHub Discussions](https://github.com/your-username/tiki-data-pipeline/discussions) |

</div>

---

## ⚠️ Important Notes

<div align="center">

> ⚠️ **Security**: File `.env` chứa thông tin nhạy cảm, **KHÔNG** commit lên Git  
> 🔒 **Production**: Thay đổi mật khẩu mặc định và sử dụng secrets management  
> 📊 **Scaling**: Cân nhắc tách riêng databases nếu cần isolation cao  
> 🐳 **Docker**: Đảm bảo đủ tài nguyên hệ thống trước khi chạy  

</div>

---

## 📝 License

<div align="center">

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</div>

---

## 🌟 Star History

<div align="center">

[![Star History Chart](https://api.star-history.com/svg?repos=your-username/tiki-data-pipeline&type=Date)](https://star-history.com/#your-username/tiki-data-pipeline&Date)

</div>

---

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,0A192F,172A45,64FFDA&height=100&section=footer"/>
  
  <p>Made with ❤️ for the Data Engineering community</p>
  
  <p>
    <img src="https://img.shields.io/github/stars/your-username/tiki-data-pipeline?style=social&label=Star"/>
    <img src="https://img.shields.io/github/forks/your-username/tiki-data-pipeline?style=social&label=Fork"/>
    <img src="https://img.shields.io/github/watchers/your-username/tiki-data-pipeline?style=social&label=Watch"/>
  </p>
</div>
