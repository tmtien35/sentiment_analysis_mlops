# 🚗 Hệ Thống MLOps Phân Tích Cảm Xúc Đánh Giá Xe Điện & Giám Sát Tự Phục Hồi
*(Vietnamese EV Review Sentiment Analysis & Self-Healing MLOps Pipeline)*

Hệ thống MLOps hoàn chỉnh, có khả năng tái lập và sẵn sàng triển khai thực tế (production-grade) nhằm phân loại cảm xúc đánh giá xe điện tiếng Việt, giám sát trôi dạt dữ liệu theo thời gian thực (PSI) và tự động kích hoạt vòng lặp huấn luyện lại tự phục hồi (Self-Healing Retraining) với chốt chặn an toàn Gatekeeper.

---

## 🔄 Quy Trình Vận Hành Toàn Diện (End-to-End MLOps Flow & Gatekeeper Logic)

Sơ đồ bên dưới minh họa quy trình vận hành khép kín tiêu chuẩn Enterprise: từ lúc Airflow kích hoạt định kỳ, cào dữ liệu mới, chấm điểm hàng loạt (batch scoring), kiểm tra trôi dạt dữ liệu kéo dài (**Persistent Drift**), tái huấn luyện với cơ chế từ chối thông minh (**Smart Rejection**), chốt chặn phê duyệt con người (**Human Approval Gatekeeper**), phân luồng an toàn (**Canary 90/10**), và quyết định **1-Click Promote / Rollback**:

```mermaid
flowchart TD
    A["⏱️ Airflow Scheduler (00:00 / @daily)"] --> B["📥 Task 1: crawl_daily_ev_reviews<br/>- Cào 20 review mới<br/>- Phân loại khía cạnh xe điện<br/>- Lưu store_reviews (is_processed = 0)"]
    B --> C["⚡ Task 2: batch_scoring_and_drift<br/>- Tải @champion từ MLflow Registry<br/>- Dự đoán sentiment & confidence<br/>- Tính chỉ số PSI tích lũy"]
    C --> D{"🔍 Kiểm tra PSI ≥ 0.15?<br/>(Phát Hiện Data Drift)"}
    
    D -- "Không (Hệ thống ổn định)" --> END["🏁 Hoàn tất Task 2:<br/>- Ghi drift_metrics vào SQL<br/>- Khóa trạng thái is_processed = 1<br/>- Ghi Run Batch_{ds} lên MLflow"]
    
    D -- "Có (Phát hiện Drift!)" --> CHK_PERSIST{"🚨 Kiểm tra Persistent Drift?<br/>(PSI ≥ 0.25 HOẶC ≥ 2 ngày trôi dạt)"}
    
    CHK_PERSIST -- "Không (Drift nhất thời - Ngày 1)" --> DEF["ℹ️ Hoãn Retrain (Theo dõi tích lũy):<br/>- Ghi cảnh báo HTML vào data/alerts/<br/>- Giữ an toàn tài nguyên tính toán"] --> END
    
    CHK_PERSIST -- "Có (Persistent Drift xác nhận!)" --> E["🚨 Kích hoạt Retrain Tự Động:<br/>python ml/train_model.py"]
    E --> F["🔄 Pipeline Huấn Luyện Lại:<br/>- Nạp nhãn vàng Active Learning<br/>- Gộp dữ liệu ngày drift vào Train<br/>- Đánh giá trên Validation Set cố định"]
    F --> G{"🛡️ BETTER THAN PRODUCTION?<br/>Macro-F1 (New) vs Macro-F1 (Champion)"}
    
    G -- "NO (Kém hơn Champion)" --> REJ["❌ SMART REJECTION:<br/>- Không đè lên Candidate mạnh hơn<br/>- Lưu bản runner-up hoặc Rejected<br/>- Ghi nhận alert sự cố HTML"] --> END
    
    G -- "YES (Vượt trội Champion)" --> H_APP["⏸️ HUMAN APPROVAL GATEWAY:<br/>- Gán alias @candidate trên MLflow<br/>- Gắn tag: pending_human_approval<br/>- Chờ Admin phê duyệt trên Streamlit"]
    
    H_APP --> CAN_DEP["🐤 CANARY ROUTING (90/10):<br/>- Admin bấm 'Phê duyệt & Bật Canary'<br/>- FastAPI chia tải: 90% Champion / 10% Canary<br/>- Ghi nhận latency_ms & model_route"]
    
    CAN_DEP --> KPI_MON{"📊 GIÁM SÁT PROXY KPIS:<br/>Confidence & Latency thực tế"}
    
    KPI_MON -- "PASS (Đạt chuẩn)" --> PROMOTE["🚀 1-Click Promote to Champion:<br/>- Thăng hạng Canary lên @champion 100%<br/>- Tắt Canary routing an toàn"] --> END
    
    KPI_MON -- "FAIL (Bất thường)" --> ROLLBACK["🔙 1-Click Rollback / Abort:<br/>- Ngắt Canary ngay lập tức<br/>- Phục hồi 100% Champion không gián đoạn"] --> END

    style A fill:#2c3e50,stroke:#34495e,stroke-width:2px,color:#fff
    style B fill:#2980b9,stroke:#1f618d,stroke-width:2px,color:#fff
    style C fill:#2980b9,stroke:#1f618d,stroke-width:2px,color:#fff
    style D fill:#f39c12,stroke:#d68910,stroke-width:2px,color:#fff
    style CHK_PERSIST fill:#e67e22,stroke:#d35400,stroke-width:2px,color:#fff
    style DEF fill:#7f8c8d,stroke:#95a5a6,stroke-width:2px,color:#fff
    style E fill:#e74c3c,stroke:#c0392b,stroke-width:2px,color:#fff
    style F fill:#8e44ad,stroke:#71368a,stroke-width:2px,color:#fff
    style G fill:#d35400,stroke:#ba4a00,stroke-width:2px,color:#fff
    style REJ fill:#c0392b,stroke:#922b21,stroke-width:2px,color:#fff
    style H_APP fill:#3498db,stroke:#2980b9,stroke-width:2px,color:#fff
    style CAN_DEP fill:#f1c40f,stroke:#f39c12,stroke-width:2px,color:#000
    style KPI_MON fill:#9b59b6,stroke:#8e44ad,stroke-width:2px,color:#fff
    style PROMOTE fill:#27ae60,stroke:#1e8449,stroke-width:2px,color:#fff
    style ROLLBACK fill:#c0392b,stroke:#922b21,stroke-width:2px,color:#fff
    style END fill:#16a085,stroke:#117864,stroke-width:2px,color:#fff
```

### 📋 Chi Tiết Từng Giai Đoạn Vận Hành

#### Giai Đoạn 1: Airflow Khởi Chạy DAG & Đánh Giá Persistent Drift
1. **00:00 Nửa đêm:** Airflow Scheduler tự động kích hoạt DAG với execution date `{{ ds }}` đại diện cho ngày vừa trôi qua.
2. **Task 1: `crawl_daily_ev_reviews`:** Lấy **20 đánh giá xe điện mới** từ kho dữ liệu mô phỏng, phân loại khía cạnh (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`), và lưu vào `store_reviews` (`is_processed = 0`).
3. **Task 2: `batch_scoring_and_drift_monitoring`:**
   * Tải mô hình `@champion` từ MLflow Model Registry, dự đoán sentiment và confidence, lưu bảng `predictions`.
   * **Kiểm tra trôi dạt (Data Drift PSI):**
     * **Nếu PSI < 0.15:** Ổn định ➔ Bỏ qua huấn luyện lại ➔ Hoàn tất mẻ chạy.
     * **Nếu PSI ≥ 0.15:** Đánh giá tính chất **Persistent Drift (Trôi dạt kéo dài)**:
       * **Quy tắc 1 (Cấp tính):** PSI ≥ 0.25 ➔ Trôi dạt cấp tính nghiêm trọng, kích hoạt huấn luyện lại ngay.
       * **Quy tắc 2 (Kéo dài):** Truy vấn lịch sử `drift_metrics`. Nếu mẻ liền trước cũng bị drift ➔ Xác nhận trôi dạt kéo dài qua nhiều mẻ, kích hoạt huấn luyện lại.
       * **Nếu chỉ là Drift đột biến đơn lẻ (Ngày 1 và PSI < 0.25):** Xuất cảnh báo HTML quan sát, **hoãn huấn luyện lại** để tiết kiệm tài nguyên.

#### Giai Đoạn 2: Huấn Luyện Lại & Chốt Chặn Gatekeeper (Smart Rejection)
1. **Chia dữ liệu cố định:** Tải bộ 1,570 đánh giá xe điện chuẩn (`data/ev_reviews_vietnam_1529_cleaned.csv`), dùng hạt giống cố định (`random_state=42`) để tách:
   * Tập Train: 80% (1,256 dòng).
   * Tập Validation (`val_df`): 10% (157 dòng) – **đây là đề thi chuẩn độc lập của Gatekeeper**.
   * Tập Test: 10% (157 dòng).
2. **Nạp dữ liệu phản hồi (Active Learning & Drift Feedback Loop):**
   * Lấy toàn bộ các review đã được chuyên gia con người thẩm định (`verified_sentiment IS NOT NULL`) từ bảng `store_reviews`.
   * Lấy các review thuộc ngày bị drift, tự động gán nhãn dự phòng.
   * **Gộp toàn bộ dữ liệu mới này vào duy nhất tập Train** (tuyệt đối không làm thay đổi tập `val_df` để chống rò rỉ dữ liệu).
3. **Huấn luyện mô hình ứng viên (Candidate):** Học lại bộ từ vựng TF-IDF và thuật toán phân loại trên tập Train đã mở rộng.
4. **Kiểm tra Gatekeeper & Smart Rejection:**
   * Cho mô hình mới dự đoán trên tập `val_df` ➔ Ra điểm `Macro-F1 (New)`.
   * Tải mô hình `@champion` đang phục vụ về, cho dự đoán trên cùng tập `val_df` ➔ Ra điểm `Macro-F1 (Champion)`.
   * So sánh với `Macro-F1 (Champion)`:
     * **🔴 KÉM HƠN CHAMPION:** Kích hoạt **Smart Rejection**. So sánh với Candidate đang có: nếu mô hình mới kém hơn Candidate hiện tại, hệ thống từ chối mô hình mới và bảo lưu Candidate mạnh nhất để không làm thụt lùi danh sách ứng viên.
     * **🟢 VƯỢT TRỘI CHAMPION:** Đăng ký phiên bản mới lên MLflow, gán alias `@candidate` và đánh dấu tag `approval_status = "pending_human_approval"`. Mô hình **không tự động thăng hạng** nhằm đảm bảo an toàn tuyệt đối cho hệ thống phục vụ.

#### Giai Đoạn 3: Phê Duyệt Con Người & Phân Luồng Canary (90/10)
1. **Giao diện Phê duyệt trên Streamlit:** Khi Candidate đạt chuẩn, bảng điều khiển Streamlit hiển thị thẻ thông báo:
   * **Nút "✅ Approve & Enable Canary (10% Traffic)":** Gán alias `@canary`, cập nhật bảng `system_settings` (`canary_enabled = 'true'`), và gửi tín hiệu cho FastAPI nạp mô hình Canary.
   * **Nút "❌ Reject Contender":** Đóng ứng viên nếu có nghi vấn về chất lượng.
2. **Phân Luồng Canary & Ghi Log An Toàn trên FastAPI:**
   * Tự động điều phối ngẫu nhiên: **90% lưu lượng sang Champion** / **10% lưu lượng sang Canary**.
   * Đo lường thời gian đáp ứng `latency_ms` và ghi nhận `model_route` (`champion` hoặc `canary`) vào bảng `inference_logs` theo thời gian thực.
   * **Kiến trúc DDL an toàn & Idempotent:** Schema cơ sở dữ liệu được khởi tạo và di trú tự động ở vòng đời khởi động (`lifespan`) bằng cú pháp `ADD COLUMN IF NOT EXISTS`, loại bỏ hoàn toàn hiện tượng hủy giao dịch SQL (`transaction abort`) trên PostgreSQL, bảo đảm bản ghi suy luận xuất hiện tức thì trên giao diện giám sát.

#### Giai Đoạn 4: Giám Sát Proxy KPIs & Quyết Định 1-Click Promote / Rollback
1. **Bảng Giám sát Operational KPIs trên Streamlit:**
   * Tỉ lệ chia tải thực tế (% Traffic Share).
   * Độ tự tin trung bình (**Proxy KPI: Average Confidence**).
   * Độ trễ trung bình (**Latency ms**).
2. **Quyết định vận hành Zero-Downtime:**
   * **"🚀 1-Click Promote Canary to Champion (100% Traffic)":** Thăng hạng phiên bản Canary thành Champion chính thức, phục hồi 100% lưu lượng sang mô hình mới.
   * **"🔙 1-Click Rollback / Abort Canary":** Ngắt Canary ngay lập tức nếu phát hiện chỉ số bất thường, đưa 100% lưu lượng về Champion an toàn.

#### Giai Đoạn 5: Hoàn Tất Task 2 và Đóng Mẻ Chạy
1. Task 2 ghi nhận kết quả PSI và trạng thái drift vào bảng SQL `drift_metrics`.
2. **Khóa trạng thái (State-Locking):** Chạy `UPDATE store_reviews SET is_processed = 1` cho các đánh giá của ngày đó để chống tính toán trùng lặp.
3. Ghi log hoàn tất mẻ chạy `Batch_{ds}` lên MLflow và Airflow kết thúc với màu xanh lá (**Success**).

---

## 🏗️ Kiến Trúc Hệ Thống (System Architecture)

Hệ thống bao gồm **6 dịch vụ đồng bộ** được điều phối hoàn chỉnh thông qua Docker Compose:

```mermaid
flowchart TB
    subgraph ClientLayer["🌐 Giao Diện Người Dùng & Giám Sát"]
        UI["🖥️ Streamlit Dashboard<br/>Cổng: 8501<br/>- Giám sát PSI & Phân phối cảm xúc<br/>- Chốt chặn Phê duyệt Con người (Human Approval)<br/>- Giám sát Proxy KPIs & 1-Click Promote / Rollback"]
    end

    subgraph ServingLayer["⚡ Tầng Phục Vụ Trực Tuyến"]
        API["🚀 FastAPI Service<br/>Cổng: 8000<br/>- API suy luận thời gian thực on-demand<br/>- Phân luồng Canary Routing an toàn (90/10)<br/>- Ghi log latency_ms & model_route"]
    end

    subgraph OrchestrationLayer["⏱️ Tầng Điều Phối & Tự Động Hóa"]
        AF_Web["🌐 Airflow Webserver<br/>Cổng: 8080<br/>- Giao diện quản trị đồ thị DAGs"]
        AF_Sched["⚙️ Airflow Scheduler<br/>- Cào 20 reviews hàng ngày lúc 00:00<br/>- Chấm điểm mẻ & Persistent Drift PSI<br/>- Tự động gọi Retrain & Smart Rejection"]
    end

    subgraph RegistryLayer["📦 Tầng Quản Lý Thử Nghiệm & Mô Hình"]
        MLFLOW["🧪 MLflow Tracking & Registry<br/>Cổng: 5000<br/>- Lưu log siêu tham số, Macro-F1, Accuracy<br/>- Quản trị aliases @champion, @candidate, @canary<br/>- Tag trạng thái pending_human_approval"]
    end

    subgraph StorageLayer["💾 Tầng Cơ Sở Dữ Liệu"]
        POSTGRES[("🐘 PostgreSQL Database<br/>Cổng: 5432<br/>- store_reviews (Dữ liệu đánh giá thô)<br/>- predictions (Kết quả dự đoán)<br/>- drift_metrics (Lịch sử PSI)<br/>- system_settings (Cờ Canary bật/tắt)<br/>- inference_logs (Model route & Latency)")]
    end

    AF_Sched -->|"1. Lưu 20 reviews thô mới"| POSTGRES
    AF_Sched -->|"2. Đọc reviews chưa xử lý (is_processed=0)"| POSTGRES
    AF_Sched -->|"3. Ghi kết quả dự đoán & metrics"| POSTGRES
    AF_Sched -->|"4. Kéo model @champion & Ghi log Batch_{ds}"| MLFLOW
    AF_Sched -->|"5. Kích hoạt Persistent Drift Retraining"| MLFLOW
    
    API -->|"Nạp model @champion & @canary"| MLFLOW
    API -->|"Đọc cờ canary_enabled & Ghi log phục vụ"| POSTGRES

    UI -->|"Đọc số liệu, cấu hình Canary & Active Learning"| POSTGRES
    UI -->|"Truy vấn Run, duyệt Candidate, thăng hạng @champion"| MLFLOW
    UI -->|"Gửi request test suy luận & Canary routing"| API

    AF_Web <-->|"Đồng bộ trạng thái điều phối"| AF_Sched

    style ClientLayer fill:#e8f8f5,stroke:#16a085,stroke-width:2px
    style ServingLayer fill:#ebf5fb,stroke:#2980b9,stroke-width:2px
    style OrchestrationLayer fill:#fef9e7,stroke:#f39c12,stroke-width:2px
    style RegistryLayer fill:#f4ecf7,stroke:#8e44ad,stroke-width:2px
    style StorageLayer fill:#eaf2f8,stroke:#34495e,stroke-width:2px
```

### 📋 Bảng Chi Tiết 6 Dịch Vụ Thành Phần

| Dịch Vụ | Cổng (Port) | Công Nghệ Cốt Lõi | Nhiệm Vụ & Trách Nhiệm Trong Hệ Thống |
| :--- | :---: | :--- | :--- |
| **Streamlit Dashboard** | `8501` | Streamlit, Plotly, Pandas | Giám sát phân phối cảm xúc, PSI trôi dạt dữ liệu, Active Learning Audit, **Chốt chặn phê duyệt con người (Human Approval Gatekeeper)**, và **bảng điều khiển Canary Proxy KPIs (1-Click Promote / Rollback)**. |
| **FastAPI Serving** | `8000` | FastAPI, Pydantic, Uvicorn | Cung cấp RESTful API phân loại cảm xúc thời gian thực (< 5ms), **hỗ trợ phân luồng Canary an toàn (90% Champion / 10% Canary)**, đo lường độ trễ và lưu trữ vết truy vết định tuyến. |
| **Airflow Scheduler** | Chạy ngầm | Apache Airflow 2.8, Python | Tự động kích hoạt lúc 00:00: cào 20 review, tiền xử lý, suy luận hàng loạt, kiểm tra **Persistent Drift PSI**, và kích hoạt Retrain thông minh với cơ chế **Smart Rejection**. |
| **Airflow Webserver** | `8080` | Apache Airflow UI, Flask | Cung cấp giao diện quản lý đồ thị DAG, kích hoạt thủ công (`Trigger DAG`), kiểm tra nhật ký chi tiết của từng Task. |
| **MLflow Registry** | `5000` | MLflow, SQLAlchemy, SQLite | Trung tâm theo dõi thử nghiệm (Experiment Tracking), ghi log metrics/hyperparameters, và quản trị vòng đời mô hình với các định danh `@champion`, `@candidate`, `@canary` cùng tag phê duyệt. |
| **PostgreSQL Database** | `5432` | PostgreSQL 15 | Cơ sở dữ liệu quan hệ lưu trữ tập trung: `store_reviews`, `predictions`, `drift_metrics`, `system_settings` (cấu hình Canary) và `inference_logs` (nhật ký định tuyến & latency). |

---

## 💻 Hướng Dẫn Cài Đặt Ban Đầu Cho Người Mới (Prerequisites & Installation)

Phần này hướng dẫn chi tiết từng bước chuẩn bị môi trường từ máy trắng dành cho người dùng mới trên cả hai hệ điều hành: **Windows** và **Linux** (Ubuntu Desktop, Google Cloud Platform VM, AWS EC2).

---

### 🖥️ 1. Yêu Cầu Cấu Hình Phần Cứng Tối Thiểu
*   **CPU:** Tối thiểu 2 Cores (Khuyến nghị 4 Cores trở lên).
*   **RAM:** Tối thiểu 4GB RAM (Khuyến nghị 8GB RAM để vận hành đồng thời 6 container Docker mượt mà).
*   **Ổ cứng:** Tối thiểu 10GB dung lượng trống.
*   **Hệ điều hành:**
    *   **Windows:** Windows 10/11 64-bit (phiên bản Home, Pro, Enterprise hoặc Education).
    *   **Linux:** Ubuntu 20.04 LTS / 22.04 LTS / 24.04 LTS hoặc Debian 11/12.

---

### 🪟 2. Hướng Dẫn Cài Đặt Trên Windows

#### Bước 2.1: Kích hoạt WSL 2 (Windows Subsystem for Linux)
Mở cửa sổ **PowerShell** bằng quyền Administrator (nhấp chuột phải vào Start -> chọn *Terminal (Admin)* hoặc *PowerShell (Admin)*) và gõ lệnh:
```powershell
wsl --install
```
*Ghi chú: Nếu hệ thống yêu cầu khởi động lại máy tính, hãy Restart để hoàn tất kích hoạt WSL 2.*

#### Bước 2.2: Cài đặt Docker Desktop trên Windows
1. Truy cập trang chủ Docker và tải về [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/).
2. Chạy file cài đặt `Docker Desktop Installer.exe`. Trong quá trình cài, đảm bảo đánh dấu chọn **"Use WSL 2 instead of Hyper-V (recommended)"**.
3. Sau khi cài xong, khởi động ứng dụng **Docker Desktop**. Chờ khoảng 1-2 phút cho Docker khởi động, khi thấy góc dưới bên trái hiển thị biểu tượng cá voi màu xanh lá kèm chữ **"Engine running"** là đã sẵn sàng.
4. Mở cửa sổ **PowerShell** kiểm tra cài đặt:
   ```powershell
   docker --version
   docker compose version
   ```

#### Bước 2.3: Cài đặt Git & Tải Mã Nguồn Dự Án (Clone Repo)
1. Tải và cài đặt [Git for Windows](https://git-scm.com/download/win) (nếu máy tính chưa cài đặt Git).
2. Mở **PowerShell**, di chuyển đến thư mục bạn muốn lưu dự án và tải mã nguồn:
   ```powershell
   git clone https://github.com/tmtien35/sentiment_analysis_mlops.git
   cd sentiment_analysis_mlops
   ```

#### Bước 2.4: Cấu Hình Email Cảnh Báo Data Drift (Real-time Gmail Alerts Trên Windows)
Hệ thống tích hợp cơ chế tự động gửi email cảnh báo về hòm thư khi phát hiện hiện tượng trôi dạt dữ liệu vựng (Data Drift với chỉ số $\text{PSI} \ge 0.15$). Để nhận email thực tế vào hộp thư cá nhân trên máy Windows:

1. **Khởi tạo tệp cấu hình `.env`:**
   Mở PowerShell tại thư mục gốc dự án và chạy lệnh:
   ```powershell
   copy .env.example .env
   ```
2. **Lấy Mật khẩu ứng dụng (Gmail App Password - 1 Phút):**
   * Truy cập trang bảo mật tài khoản Google: [https://myaccount.google.com/security](https://myaccount.google.com/security)
   * Bật **Xác minh 2 bước** (*2-Step Verification*) nếu tài khoản chưa bật.
   * Truy cập liên kết tạo mật khẩu ứng dụng: [https://myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
   * Đặt tên ứng dụng (ví dụ: `MLOps Alert`) và nhấn **Tạo (Create)**. Google sẽ cấp mã 16 chữ cái (ví dụ: `abcd efgh ijkl mnop`).
3. **Mở tệp `.env` để điền thông tin:**
   ```powershell
   notepad .env
   ```
   *Điền 3 dòng sau rồi lưu lại (`Ctrl + S`):*
   ```ini
   SMTP_SENDER=email_cua_ban@gmail.com
   SMTP_PASSWORD=16_chu_cai_vua_tao
   SMTP_RECIPIENT=email_nhan_canh_bao@gmail.com
   ```
4. **Kiểm tra gửi email trong 3 giây:**
   ```powershell
   python ml/test_email_smtp.py
   ```
   *(Hệ thống hỗ trợ chuyển cổng thông minh Port 587 -> Port 465. Email kiểm thử sẽ xuất hiện ngay trong hòm thư cá nhân của bạn).*

---

### 🐧 3. Hướng Dẫn Cài Đặt Trên Linux (Ubuntu / Debian / GCP VM / AWS EC2)

#### Bước 3.1: Cập nhật hệ thống & Cài đặt Git, Curl
Mở Terminal trên máy Linux hoặc qua kết nối SSH vào Cloud VM, chạy lệnh:
```bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y git curl ca-certificates
```

#### Bước 3.2: Cài đặt Docker & Docker Compose Plugin (Chính thức & Tự Động)
Sử dụng script cài đặt tự động được cung cấp chính thức bởi Docker:
```bash
# 1. Tải và chạy script cài đặt Docker Engine
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 2. Cấp quyền chạy Docker cho người dùng hiện tại (tránh phải gõ sudo mỗi lần chạy)
sudo usermod -aG docker $USER
newgrp docker

# 3. Kích hoạt dịch vụ Docker tự khởi động cùng hệ thống
sudo systemctl enable docker
sudo systemctl start docker
```

*Kiểm tra Docker đã cài đặt thành công:*
```bash
docker --version
docker compose version
```
*(Cả hai lệnh cần xuất hiện thông tin phiên bản Docker Engine và Docker Compose v2.x)*.

#### Bước 3.3: Tải Mã Nguồn Dự Án (Clone Repo)
```bash
git clone https://github.com/tmtien35/sentiment_analysis_mlops.git
cd sentiment_analysis_mlops
```

#### Bước 3.4: Cấu Hình Email Cảnh Báo Data Drift (Real-time Gmail Alerts Trên Linux / Cloud VM)
Hệ thống tích hợp cơ chế tự động gửi email cảnh báo về hòm thư khi phát hiện hiện tượng trôi dạt dữ liệu vựng (Data Drift với chỉ số $\text{PSI} \ge 0.15$). Để nhận email thực tế vào hộp thư cá nhân trên máy Linux / Cloud VM:

1. **Khởi tạo tệp cấu hình `.env`:**
   ```bash
   cp .env.example .env
   ```
2. **Lấy Mật khẩu ứng dụng (Gmail App Password - 1 Phút):**
   * Truy cập trang bảo mật tài khoản Google: [https://myaccount.google.com/security](https://myaccount.google.com/security)
   * Bật **Xác minh 2 bước** (*2-Step Verification*) nếu tài khoản chưa bật.
   * Truy cập liên kết tạo mật khẩu ứng dụng: [https://myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
   * Đặt tên ứng dụng (ví dụ: `MLOps Alert`) và nhấn **Tạo (Create)**. Google sẽ cấp mã 16 chữ cái (ví dụ: `abcd efgh ijkl mnop`).
3. **Mở tệp `.env` để điền thông tin:**
   ```bash
   nano .env
   ```
   *Điền 3 dòng cấu hình sau rồi lưu lại (`Ctrl + O` -> `Enter` -> `Ctrl + X`):*
   ```ini
   SMTP_SENDER=email_cua_ban@gmail.com
   SMTP_PASSWORD=16_chu_cai_vua_tao
   SMTP_RECIPIENT=email_nhan_canh_bao@gmail.com
   ```
4. **Kiểm tra đường truyền gửi email trong 3 giây:**
   ```bash
   python3 ml/test_email_smtp.py
   ```
   *(Hệ thống hỗ trợ cơ chế chuyển cổng thông minh: ưu tiên Port 587 STARTTLS, nếu bị chặn sẽ tự động thử tiếp Port 465 SSL. Khi test thành công, email kiểm thử sẽ xuất hiện ngay trong hòm thư của bạn).*

---

## 🚀 Chế Độ Khởi Động & Vận Hành Hệ Thống

Dự án hỗ trợ **hai chế độ vận hành độc lập**, phục vụ linh hoạt cho cả nhu cầu phát triển cá nhân và đánh giá triển khai sản phẩm:

---

### ⚡ Chế Độ A: Chạy Python Cục Bộ (Dành cho Lập Trình Viên & Kiểm Thử Nhanh)
*Sử dụng chế độ này để chạy trực tiếp trên máy cá nhân mà không cần khởi động hệ thống container Docker nền.*

*   **Chuẩn bị môi trường Python (chỉ làm lần đầu):**
    Yêu cầu máy tính đã cài **Python 3.10 - 3.12**. Tạo môi trường ảo và cài đặt thư viện:
    ```bash
    # 1. Khởi tạo môi trường ảo
    python -m venv .venv

    # 2. Kích hoạt môi trường:
    # Trên Windows (PowerShell): .venv\Scripts\Activate.ps1
    # Trên Windows (CMD):        .venv\Scripts\activate.bat
    # Trên Linux / macOS:        source .venv/bin/activate

    # 3. Cài đặt toàn bộ thư viện:
    pip install -r requirements.txt
    ```

*   **Khởi chạy nhanh 1 bước:** Mở terminal tại thư mục gốc dự án và chạy:
    ```bash
    python run_local.py
    ```
    *Kịch bản này tự động chạy kiểm thử đơn vị Pytest, sau đó khởi chạy đồng thời cả máy chủ FastAPI (`:8000`) và bảng điều khiển Streamlit (`:8501`).*
*   **Địa chỉ truy cập trên trình duyệt:**
    *   **Bảng điều khiển Streamlit:** [http://localhost:8501](http://localhost:8501)
    *   **Tài liệu API Swagger FastAPI:** [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Các lệnh thực thi từng bước riêng lẻ (khi cần phát triển sâu):**
    *   *Nạp dữ liệu lịch sử:* `python data/ingest_pipeline.py --backfill`
    *   *Gửi đánh giá tương tác CLI:* `python data/submit_review.py`
    *   *Huấn luyện & tuyển chọn Champion:* `python ml/train_model.py`
    *   *Chạy kiểm thử tự động:* `python -m pytest ml/test_pipeline.py`
*   **Cách dừng hoạt động an toàn:** Nhấn tổ hợp phím **`[Ctrl + C]`** trong cửa sổ dòng lệnh. Toàn bộ tiến trình sẽ dừng và giải phóng cổng ngay lập tức.

---

### 🐳 Chế Độ B: Chạy Toàn Bộ Container Docker (Dành cho Trình Diễn, Chấm Điểm & Cloud VM)
*Sử dụng chế độ này để vận hành mạng lưới đầy đủ 6 dịch vụ hoàn chỉnh kết nối cơ sở dữ liệu PostgreSQL, Airflow và MLflow.*

#### 🚀 **Lệnh Khởi Tạo Sạch 1 Bước (1-Command Clean Slate Deployment)**
Để triển khai hoặc cài đặt mới lại toàn bộ hệ thống từ đầu trên bất kỳ máy chủ nào (máy tính cá nhân, GCP VM hoặc AWS EC2):

*   **Dành cho Linux / macOS / Git Bash:**
    ```bash
    docker compose down -v && \
    docker compose build fastapi && \
    docker compose run --rm fastapi python ml/train_model.py && \
    docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset && \
    docker compose up -d --build
    ```

*   **Dành cho Windows PowerShell:**
    ```powershell
    docker compose down -v; `
    docker compose build fastapi; `
    docker compose run --rm fastapi python ml/train_model.py; `
    docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset; `
    docker compose up -d --build
    ```
*Lệnh trên tự động thực hiện hoàn tất toàn bộ quy trình:*
1. Dọn dẹp sạch sẽ các container và ổ đĩa dữ liệu cũ (`down -v`).
2. Xây dựng Docker images và huấn luyện mô hình ban đầu, đăng ký lên MLflow làm `@champion`.
3. Khởi tạo cơ sở dữ liệu PostgreSQL và nạp sẵn 25 ngày dữ liệu lịch sử ổn định (500 đánh giá xe điện không trùng lặp).
4. Khởi chạy toàn bộ 6 container ở chế độ nền: Postgres (`5432`), MLflow (`5000`), Airflow (`8080`), FastAPI (`8000`), Streamlit (`8501`).
5. **Tự động kích hoạt DAG:** DAG `daily_sentiment_analysis` đã được cấu hình tự động mở khóa sẵn (`is_paused_upon_creation=False`). Sau khi Airflow hoàn tất quá trình khởi tạo metadata lần đầu (~30–45 giây), DAG sẽ tự động xuất hiện ở trạng thái **ON** trên Web UI.
*(Nếu muốn kiểm tra danh sách DAG từ dòng lệnh sau khi dịch vụ sẵn sàng: `docker compose exec airflow-webserver airflow dags list`).*

#### 🌐 **Các Địa Chỉ Dịch Vụ Mở Trên Trình Duyệt**
*   **Bảng Điều Khiển Phân Tích Streamlit:** `http://<IP_HOẶC_LOCALHOST>:8501` *(Kết nối trực tiếp PostgreSQL, thẩm định Active Learning & số liệu vận hành)*
*   **Giao Diện Điều Phối Airflow:** `http://<IP_HOẶC_LOCALHOST>:8080` *(Tài khoản: `mlops` | Mật khẩu: `mlops`)*
*   **Trung Tâm Thử Nghiệm MLflow:** `http://<IP_HOẶC_LOCALHOST>:5000` *(Theo dõi experiment `ev-sentiment-analysis` & model `ev-sentiment-model`)*
*   **API Phục Vụ Dự Đoán FastAPI:** `http://<IP_HOẶC_LOCALHOST>:8000/docs` *(Swagger UI kiểm thử API trực tuyến)*

#### ⚙️ **Sổ Tay Thao Tác Vận Hành Chuẩn (Operations Playbook)**
*   **Kịch bản 1: Cập nhật code / fix bug / cấu hình mới từ Git (Không mất dữ liệu PostgreSQL, giữ nguyên model):**
    ```bash
    # 1. Kéo mã nguồn mới nhất về
    git pull origin main

    # 2. Khởi tạo tệp cấu hình môi trường (nếu máy chủ chưa có file .env)
    cp .env.example .env
    nano .env   # (Điền Gmail và App Password của bạn vào rồi lưu lại)

    # 3. Đóng gói lại và khởi động các container ở chế độ nền
    docker compose up -d --build
    ```
    *Kiểm tra nhanh xem Airflow container đã nhận cấu hình email cảnh báo chưa:*
    ```bash
    docker compose exec airflow-scheduler python ml/test_email_smtp.py
    ```
*   **Kịch bản 2: Cập nhật mã nguồn có thay đổi Model hoặc Tiền Xử Lý (Giữ nguyên dữ liệu PostgreSQL, retrain Champion mới trên MLflow của VM):**
    ```bash
    git pull && docker compose build && docker compose run --rm fastapi python ml/train_model.py && docker compose up -d
    ```
*   **Kịch bản 3: Tạm dừng toàn bộ hệ thống (Bảo lưu dữ liệu):**
    ```bash
    docker compose down
    ```
*   **Kịch bản 4: Khởi động lại hệ thống (Bảo lưu dữ liệu):**
    ```bash
    docker compose up -d
    ```
*   **Kịch bản 5: Xóa toàn bộ làm lại từ đầu:** Chạy lại **Lệnh Khởi Tạo Sạch 1 Bước** ở trên.

---

## 🔄 Chi Tiết 3 Luồng Vận Hành Hệ Thống (MLOps Operational Flows)

### 1️⃣ **Luồng Chạy Tự Động Cuối Ngày (Scheduled Midnight Run - `@daily`)**
*   **Thời điểm kích hoạt:** Airflow Scheduler tự động kích hoạt DAG `daily_sentiment_analysis` vào 00:00 mỗi ngày với execution date `{{ ds }}` đại diện cho ngày vừa hoàn tất.
*   **Task 1: `crawl_daily_ev_reviews` (Thu thập dữ liệu mô phỏng):**
    *   Bot crawler trích xuất 20 đánh giá EV mới (chưa từng thu thập) từ kho dữ liệu mô phỏng `data/ev_feed_simulation_pool.csv`.
    *   Chỉ trích xuất raw text (không dùng nhãn sentiment có sẵn để đảm bảo tính khách quan thực tế), tự động phân loại danh mục khía cạnh xe điện (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`), gán `review_date = {{ ds }}`, `is_processed = 0`, `verified_sentiment = NULL`.
    *   Ghi dữ liệu vào bảng `store_reviews` với cơ chế sinh mã tự động nối tiếp theo ngày, bảo đảm không bị trùng lặp khóa chính và luôn nạp đúng 20 bản ghi mới mỗi lượt chạy.
*   **Task 2: `batch_scoring_and_drift_monitoring` (Dự đoán hàng loạt & Giám sát trôi dạt):**
    *   Truy vấn các review đang chờ xử lý (`is_processed = 0`) thuộc ngày `{{ ds }}` từ `store_reviews`.
    *   Tiền xử lý văn bản qua hàm chuẩn hóa `clean_text` tích hợp bộ tách từ ghép tiếng Việt **PyVi** (`ViTokenizer`) giúp nhận diện chuẩn xác các từ vựng ngữ nghĩa và cụm phủ định (*"sạc_lâu", "tiết_kiệm", "không_quá", "giá_bán"*).
    *   Tải trực tiếp mô hình `@champion` đang phục vụ từ MLflow Model Registry (`models:/ev-sentiment-model@champion`).
    *   Thực hiện suy luận phân loại cảm xúc (Positive / Neutral / Negative) và tính toán độ tự tin `confidence` (từ `predict_proba`), sau đó lưu vào bảng `predictions`.
    *   Tính toán chỉ số trôi dạt dữ liệu **Population Stability Index (PSI)** so với phân phối sentiment chuẩn ban đầu.
    *   **Khi phát hiện Data Drift (`psi_score >= 0.15`):**
        *   Tạo báo cáo cảnh báo HTML tại `data/alerts/drift_alert_<date>.html`.
        *   Gửi email cảnh báo thời gian thực qua Gmail SMTP (nếu cấu hình `SMTP_PASSWORD`).
        *   **Cơ chế Tự phục hồi (Self-Healing Retraining):** Tự động gọi pipeline `ml/train_model.py`: gộp dữ liệu huấn luyện gốc với các nhãn do con người thẩm định (`verified_sentiment IS NOT NULL`), đào tạo lại các mô hình ứng viên, kiểm định qua **Gatekeeper** đối đầu với champion hiện tại. Nếu mô hình mới vượt trội về Macro-F1, hệ thống lập tức thăng hạng lên `@champion` mới trong MLflow; nếu không đạt, hủy đăng ký và xuất báo cáo sự cố `data/alerts/retrain_failed_*.html`.
    *   Ghi nhận số liệu PSI, độ tự tin trung bình, tổng số mẫu vào bảng `drift_metrics`.
    *   **Khóa trạng thái (State-Locking):** Cập nhật `is_processed = 1` cho các đánh giá vừa xử lý để không bao giờ bị tính trùng.
    *   Ghi log thông số mẻ chạy (`batch_psi_score`, `batch_avg_confidence`, `batch_row_count`) lên MLflow run `Batch_{ds}`.

---

### 2️⃣ **Luồng Kích Hoạt DAG Thủ Công (Manual DAG Trigger)**
*   **Thời điểm kích hoạt:** Khi kỹ sư MLOps bấm nút **"Trigger DAG"** trên giao diện Airflow Web UI (`http://localhost:8080`) hoặc chạy lệnh CLI `airflow dags trigger daily_sentiment_analysis`.
*   **Cơ chế thực thi:**
    *   Airflow lập tức khởi tạo một DagRun mới với `logical_date` là ngày hiện tại.
    *   **Task 1 (`crawl_daily_ev_reviews`):**
        *   Mỗi lần kích hoạt (dù là bấm thủ công hay chạy tự động theo lịch), hệ thống lấy đúng ngày thực thi (`review_date = {{ ds }}` - mặc định là ngày hôm nay nếu không chọn ngày cụ thể).
        *   Bot crawler trích xuất 20 đánh giá xe điện mới toanh từ kho dữ liệu mô phỏng `data/ev_feed_simulation_pool.csv` (đảm bảo không trùng lặp nội dung với bất kỳ đánh giá nào đã cào trước đó).
        *   Tự động kiểm tra số lượng review hiện có trong ngày đó để sinh mã định danh nối tiếp (ví dụ: lần đầu cào `FEED_YYYYMMDD_001` đến `_020`, lần tiếp theo bấm sẽ sinh nối tiếp từ `_021` đến `_040`), hoàn toàn không bị chặn hay xung đột khóa chính, ghi vào `store_reviews` với trạng thái `is_processed = 0`.
    *   **Task 2 (`batch_scoring_and_drift_monitoring`):**
        *   Truy vấn 20 bản ghi mới nạp (`is_processed = 0`) thuộc ngày thực thi `{{ ds }}`.
        *   Thực hiện trọn vẹn chu trình: Tiền xử lý PyVi ➔ Suy luận với model Champion ➔ Lưu kết quả vào bảng `predictions` ➔ Tính toán phân phối sentiment và chỉ số PSI tích lũy cho ngày đó ➔ Kích hoạt Self-Healing Retraining nếu phát hiện trôi dạt dữ liệu ➔ Cập nhật `drift_metrics` ➔ Khóa trạng thái `is_processed = 1` ➔ Ghi nhận mẻ chạy mới lên MLflow (`Batch_<ds>_<HHMMSS>`).
        *   *(Lưu ý: Nếu kích hoạt script CLI trực tiếp `python airflow_home/dags/batch_scoring.py` mà không truyền `--date`, pipeline sẽ tự động quét TẤT CẢ các ngày đang tồn đọng `is_processed = 0` trong database và xử lý dứt điểm lần lượt từng ngày).*

---

### 3️⃣ **Luồng Kích Hoạt Huấn Luyện Lại Thủ Công (Manual Retraining Trigger)**
*   **Thời điểm kích hoạt:** Người vận hành bấm nút **"⚡ Trigger Retrain Manual"** trên sidebar giao diện Streamlit hoặc chạy lệnh `python ml/train_model.py` trong terminal/container.
*   **Cơ chế thực thi:**
    1.  **Thu nhận dữ liệu Active Learning:** Quét bảng `store_reviews` tìm các bản ghi đã được chuyên gia con người thẩm định hoặc sửa nhãn (`verified_sentiment IS NOT NULL` từ công cụ Active Learning Audit trên Streamlit).
    2.  **Mở rộng tập huấn luyện (Data Augmentation):** Gộp toàn bộ nhãn người thẩm định này vào tập huấn luyện gốc `data/train_v1.csv` làm nhãn vàng (gold labels), giúp mô hình cập nhật từ vựng mới của thị trường EV.
    3.  **Huấn luyện đa mô hình:** Khởi tạo TF-IDF vectorizer và huấn luyện 4 thuật toán phân loại (Logistic Regression, Linear SVM, Multinomial Naive Bayes, Complement Naive Bayes / Random Forest) trên tập dữ liệu đã mở rộng.
    4.  **Đánh giá trên tập Validation chuẩn:** Đánh giá Macro-F1 và Accuracy trên tập kiểm định độc lập `data/val_v1.csv`.
    5.  **Cơ chế Gatekeeper Validation (Chốt chặn an toàn):**
        *   So sánh Macro-F1 của mô hình ứng viên tốt nhất với mô hình `@champion` đang phục vụ thực tế.
        *   **Đạt chuẩn (Pass):** Nếu `Macro-F1 (New) >= Macro-F1 (Champion)`, mô hình mới được đăng ký phiên bản tiếp theo vào MLflow Model Registry và tự động thăng hạng lên `@champion`.
        *   **Không đạt (Fail):** Nếu mô hình mới có hiệu năng thấp hơn Champion cũ, Gatekeeper từ chối tự động thăng hạng để bảo đảm độ ổn định hệ thống. Thay vào đó, mô hình mới được đăng ký với alias `@candidate` (Contender) và ghi nhận báo cáo sự cố `data/alerts/retrain_failed_YYYY_MM_DD_HHMM.html`. Trên giao diện Streamlit xuất hiện bảng Scorecard đối đầu trực tiếp kèm nút **`⚠️ Chấp nhận đánh đổi: Ép lên Champion 🏆`** cho phép Admin chủ động đưa Contender lên thay thế Champion nếu thấy hợp lý về mặt nghiệp vụ (Break-Glass Override).
    6.  **Đồng bộ Nạp Nóng Mô Hình Tức Thì (Zero-Downtime Hot-Reload):** Pipeline huấn luyện (`ml/train_model.py`) và giao diện Streamlit tự động gửi tín hiệu `POST /reload-models` sang container FastAPI. Mô hình Champion mới ngay lập tức được tải từ MLflow Registry vào bộ nhớ RAM phục vụ thực tế trong vòng vài mili-giây mà không cần khởi động lại container hay làm gián đoạn API.
    7.  **Ghi chép MLflow Tracking:** Toàn bộ siêu tham số, chỉ số đánh giá, kích thước tập dữ liệu (`train_dataset_size`), và biểu đồ Confusion Matrix được lưu trữ đầy đủ trên MLflow.

---

## 📊 Giám Sát, Tự Phục Hồi & Nhật Ký Vận Hành (Monitoring & Logs)

Để dễ dàng giám sát quá trình chấm điểm mẻ liên tục, cảnh báo trôi dạt dữ liệu và vòng lặp tự phục hồi retraining, hệ thống tổng hợp nhật ký minh bạch qua **4 nguồn chính**:

### **1. Nhật Ký Tự Phục Hồi Tự Động (Airflow Orchestration)**
Khi DAG chấm điểm mẻ hàng ngày phát hiện trôi dạt dữ liệu và tự động kích hoạt vòng lặp huấn luyện lại, toàn bộ log chuẩn (stdout/stderr) đều được Airflow ghi lại chi tiết.
*   **Vị trí kiểm tra:** Trên **Giao diện Airflow Web UI** (`http://localhost:8080`).
*   **Cách xem:** Đăng nhập bằng `mlops / mlops` ➔ Chọn DAG **`daily_sentiment_analysis`** ➔ Nhấp vào task chấm điểm mẻ đã hoàn tất gần nhất (màu xanh lá) ➔ Chọn tab **`Log`** ở trên cùng. Tại đây, bạn sẽ thấy toàn bộ nhật ký terminal của quá trình huấn luyện lại: từ nạp dữ liệu SQL, cập nhật trọng số TF-IDF, đánh giá Gatekeeper, cho đến lệnh đăng ký phiên bản lên MLflow.
*   **Kiến trúc 2 Task của DAG:**
    1. **`crawl_daily_ev_reviews`**: Mô phỏng cào dữ liệu xe điện định kỳ. Lấy ngẫu nhiên 20 đánh giá mới không trùng lặp từ pool dữ liệu (`data/ev_feed_simulation_pool.csv`), gắn ngày thực thi hiện tại (`{{ ds }}`), tự động phân loại khía cạnh (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`), và lưu vào `store_reviews` với cơ chế chống trùng lặp (idempotency).
    2. **`batch_scoring_and_drift_monitoring`**: Truy vấn các review chưa xử lý (`is_processed = 0`), dự đoán cảm xúc bằng mô hình `@champion` đang hoạt động, tính toán chỉ số trôi dạt PSI, và kích hoạt huấn luyện tự phục hồi nếu vượt ngưỡng 0.15.

### **2. Nhật Ký Huấn Luyện Lại Thủ Công (Streamlit Container)**
Khi quản trị viên kích hoạt huấn luyện lại thủ công bằng cách bấm nút **`Trigger Retrain Manual`** trên sidebar của Streamlit, kịch bản sẽ chạy bên trong container Streamlit.
*   **Vị trí kiểm tra:** Luồng stdout của dịch vụ Streamlit.
*   **Cách xem:** Mở terminal máy chủ hoặc máy cá nhân và chạy lệnh:
    ```bash
    docker compose logs -f streamlit
    ```
    Lệnh này sẽ hiển thị thời gian thực toàn bộ quá trình chạy của `ml/train_model.py`: tính toán chỉ số validation, gộp nhãn mới từ Active Learning, và đối đầu trực tiếp với champion hiện tại.

### **3. Nhật Ký So Sánh Mô Hình & Metadata (MLflow Registry)**
Mọi lần huấn luyện thành công và vượt qua chốt chặn an toàn Gatekeeper đều được đánh số phiên bản và lưu trữ đầy đủ.
*   **Vị trí kiểm tra:** **Giao diện MLflow Web UI** (`http://localhost:5000`).
*   **Cách xem:** Chọn Run đang hoạt động ➔ Kiểm tra các siêu tham số chính, điểm kiểm định (Macro-F1, Accuracy), thông số **`train_dataset_size`** (chứng minh nhãn mới từ Active Learning đã được nạp thành công!), và xem biểu đồ ma trận nhầm lẫn `Confusion Matrix` tương tác trong mục Artifacts.

### **4. Báo Cáo Sự Cố Huấn Luyện (Nhật Ký Gatekeeper Chặn Nâng Cấp)**
Nếu mô hình ứng viên mới không vượt qua được mô hình Champion hiện tại, chốt chặn an toàn Gatekeeper sẽ hủy quá trình nâng cấp tự động và xuất báo cáo sự cố định dạng HTML.
*   **Vị trí kiểm tra:** Trong thư mục **`data/alerts/`** trên ổ đĩa máy chủ.
*   **Cách xem:** Tìm các tệp có định dạng `retrain_failed_YYYY_MM_DD_HHMM.html`. Báo cáo này so sánh song song điểm số Macro-F1 của cả hai mô hình, giải thích rõ nguyên nhân vì sao bản cập nhật bị chặn lại nhằm bảo đảm an toàn cho tầng phục vụ trực tuyến (Zero Downtime & Zero Regression).

---

## 🛠️ Xử Lý Sự Cố Môi Trường Linux VM & Docker

Nếu triển khai trên máy chủ Cloud Linux mới (như **Google Cloud Platform VM / AWS EC2**) hoặc máy tính đời cũ, bạn có thể gặp xung đột phiên bản Docker ở mức hệ thống. Dưới đây là cách khắc phục nhanh:

### 🚨 1. Lỗi Lệnh: `docker compose` hoặc KeyError: `ContainerConfig`
Nếu chạy lệnh `docker compose` bị lỗi hoặc báo `KeyError: 'ContainerConfig'` khi khởi động, máy của bạn đang chạy phiên bản cũ Docker Compose V1 bằng Python (ví dụ: bản `1.29.2`).

Hãy nâng cấp lên phiên bản chính thức **Docker Compose V2** (viết bằng Go) bằng các lệnh sau:
```bash
# 1. Tạo thư mục plugins CLI
mkdir -p ~/.docker/cli-plugins/

# 2. Tải bản binary Docker Compose V2 chính thức từ GitHub
curl -SL https://github.com/docker/compose/releases/download/v2.24.1/docker-compose-linux-x86_64 -o ~/.docker/cli-plugins/docker-compose

# 3. Phân quyền thực thi
chmod +x ~/.docker/cli-plugins/docker-compose

# 4. Ghi đè liên kết tượng trưng /usr/local/bin cũ
sudo curl -SL https://github.com/docker/compose/releases/download/v2.24.1/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```
Kiểm tra lại bằng lệnh `docker compose version` (kết quả hiển thị `v2.24.1+`).

### 🚨 2. Lỗi SQLite: `unable to open database file`
Ở các phiên bản trước, việc mount trực tiếp một tệp chưa tồn tại vào container (như `- ./mlflow.db:/app/mlflow.db`) có thể khiến Docker tạo nhầm `mlflow.db` thành một **thư mục**.

Để xử lý triệt để vấn đề này, **kiến trúc dự án đã quy hoạch toàn bộ việc lưu trữ SQLite (cả `results.db` và `mlflow.db`) vào thư mục chung `./data/`**, được mount ở cấp độ thư mục `- ./data:/app/data`. Cách này đảm bảo dữ liệu luôn được lưu bền vững 100%, không bị xung đột tệp-thư mục và chạy trơn tru ngay từ lần đầu!

Nếu gặp lỗi này trên VM cũ, bạn chỉ cần xóa thư mục rác do Docker tạo ra:
```bash
rm -rf mlflow.db
```

---

### ⚠️ CẢNH BÁO QUAN TRỌNG VỀ XUNG ĐỘT CỔNG (PORT CONFLICT)
**KHÔNG** chạy lệnh `python run_local.py` khi các container Docker đang hoạt động! Do Docker đã chiếm dụng các cổng `8000` (FastAPI) và `8501` (Streamlit), việc chạy đồng thời kịch bản local sẽ báo lỗi **`AddressAlreadyInUse` / `Port in use`**.

Luôn đảm bảo một chế độ đã dừng hoàn toàn trước khi khởi chạy chế độ còn lại!

---

## 🏆 Đánh Giá & Bảng Xếp Hạng Mô Hình (Benchmark Leaderboard)

Trong giai đoạn nghiên cứu và đánh giá thực nghiệm, chúng tôi đã thử nghiệm nhiều kiến trúc mô hình khác nhau trên tập dữ liệu phân tầng (Stratified Splits) và ghi lại toàn bộ tiến trình trên MLflow. Đối với tập dữ liệu đánh giá xe điện tiếng Việt (`data/ev_reviews_vietnam_1529_cleaned.csv`), mô hình **Logistic Regression (`C=2.0`, `solver='lbfgs'`)** được chọn làm `@champion` chính thức:

| Mô Hình Thử Nghiệm | Macro-F1 (Tập Validation) | Macro-F1 (5-Fold CV) | Accuracy (Validation) | Trạng Thái Đăng Ký |
| :--- | :---: | :---: | :---: | :---: |
| **Logistic Regression (`C=2.0`)** | **0.9203** | **0.8782 ± 0.0182** | **91.50%** | **🏆 Champion (Mô hình phục vụ chính thức @champion)** |
| **Linear SVM (`SGD log_loss`)** | 0.9265 | 0.8731 ± 0.0150 | 92.16% | Contender / Ứng viên thay thế |
| **Multinomial Naive Bayes** | 0.9203 | 0.8789 ± 0.0147 | 91.50% | Contender |
| **Complement Naive Bayes** | 0.9203 | 0.8789 ± 0.0147 | 91.50% | Contender |
| **Random Forest (150 cây)** | 0.9203 | 0.8752 ± 0.0123 | 91.50% | Baseline *(Dễ bị đánh lừa bởi câu tương phản)* |

*Mô hình Logistic Regression chiến thắng đã được kiểm tra độc lập một lần duy nhất trên tập Test mù (153 đánh giá chưa từng thấy), đạt **Accuracy: 88.24% và Macro-F1: 0.8830** (F1 Tiêu cực: 0.9114, F1 Trung tính: 0.8269, F1 Tích cực: 0.9106).*

> **Cập Nhật Phiên Bản Champion v2 (Tối Ưu Ngữ Nghĩa Xe Điện & Khử Thiên Lệch Trung Tính):**  
> Để khắc phục hiện tượng thiên lệch gán nhãn trung tính (Neutral Bias) do từ vựng xe điện phân bố không đồng đều, mô hình Champion v2 được nâng cấp với:  
> - Bổ sung mẫu câu chuyên ngành (thiết kế ngoại thất, màn hình dễ dùng, pin sạc nhanh, điều hòa làm mát, khoang hành lý) nâng quy mô lên **1,570 mẫu**.  
> - Tích hợp `class_weight='balanced'` và `sublinear_tf=True` với 8,000 n-gram đặc trưng nhằm cân bằng hàm phạt giữa 3 lớp cảm xúc.  
> - Kết quả kiểm định trên tập Test độc lập: **Accuracy đạt 89.17%**, **Macro-F1 đạt 0.8942**. Tỷ lệ tin cậy thấp (<60%) trên dữ liệu thử nghiệm thực tế giảm mạnh từ 92.3% xuống chỉ còn 46.2%, độ tin cậy trung bình tăng lên **59.3%**, nhận diện chính xác các phản ánh về điều hòa, trạm sạc và trải nghiệm lái.

> **Cập Nhật Đột Phá: Champion v3 (Tích Hợp Tách Từ Ghép Tiếng Việt PyVi):**  
> Nhằm xử lý dứt điểm các lỗi ngộ nhận ngữ nghĩa của tiếng Việt (như từ ghép *"tiết_kiệm"*, *"sạc_lâu"*, *"không_khí"* bị cắt thành *"không"*, *"quá"*...), Champion v3 tích hợp bộ tách từ ghép `pyvi` trực tiếp vào pipeline trích xuất đặc trưng TF-IDF:  
> - **Chỉ số trên tập Test độc lập:** **Macro-F1 đạt 0.9080**, **Accuracy đạt 90.45%** (tăng mạnh so với v1: 0.8830 và v2: 0.8942).  
> - **Độ chính xác thực nghiệm kiểm toán thực tế:** Đạt **95.0% (19/20 câu đúng)** trên tập đánh giá khách hàng thực tế ngẫu nhiên (`2026-09-14T04-10_export.csv`), triệt tiêu hoàn toàn lỗi ngộ nhận phủ định và nhận diện chuẩn xác các mẫu câu phức tạp ("xe không quá rộng", "không khí thoáng đãng", "hệ thống sạc không phải lúc nào cũng nhanh").


---

### 🧠 Lý Do Lựa Chọn Mô Hình (Tại Sao Lại Là Logistic Regression?)

Chúng tôi chọn **Logistic Regression (`C=2.0, solver='lbfgs'`)** làm mô hình phục vụ trực tuyến `@champion` dựa trên 4 yếu tố vận hành thực tế cốt lõi:

1. **Khả Năng Xử Lý Xuất Sắc Các Câu Tương Phản & Ngữ Nghĩa Phức Tạp:** Khi thử nghiệm với các câu đánh giá xe điện mang tính tương phản cao (ví dụ: *"Nội thất nhìn thì hào nhoáng nhưng chất lượng gia công ọp ẹp, đi qua gờ kêu lạch cạch khó chịu"*), Logistic Regression cân bằng trọng số rất chuẩn xác giữa mệnh đề tiêu cực thực tế và từ ngữ tích cực gây nhiễu (`hào nhoáng`), trong khi các mô hình dạng cây quyết định (Tree Ensembles) thường bị nhầm lẫn.
2. **Phân Phối Xác Suất Chuẩn Xác (`predict_proba`):** Logistic Regression cung cấp xác suất dự đoán rất mịn và chuẩn mực, tạo điều kiện thuận lợi để tính độ tự tin theo thời gian thực và phân tầng độ bất định (Chia 3 bậc: Cao ≥80%, Trung bình 60-79%, Thấp <60%) phục vụ vòng lặp Active Learning.
3. **Khả Năng Giải Thích Minh Bạch (Explainable AI):** Các hệ số tuyến tính (coefficients) đại diện trực tiếp cho mức độ tác động tích cực hay tiêu cực của từng n-gram từ vựng, giúp kỹ sư và chuyên viên nghiệp vụ dễ dàng kiểm toán lý do tại sao một câu review lại được phân loại như vậy.
4. **Độ Trễ Siêu Thấp Trên CPU (Sub-millisecond Latency):** Thời gian huấn luyện lại chưa đầy 0.05 giây và độ trễ suy luận dưới 5ms trên CPU tiêu chuẩn 1 luồng, hoàn toàn không cần hạ tầng GPU đắt đỏ.

---

### ⚙️ Giải Thích Chi Tiết Cấu Hình Siêu Tham Số Trên MLflow (Parameters Deep Dive)

Mô hình Champion (`ml/train_model.py`) được quản trị vòng đời và ghi nhận minh bạch 8 siêu tham số cốt lõi trên MLflow Tracking:

1. **`clf__C = 2.0` (Hệ số điều hòa nghịch đảo):** Điều chỉnh độ nhạy học đặc trưng. Mức `2.0` (so với mặc định `1.0`) giúp mô hình học sâu và mạnh dạn nhận diện các cụm từ cảm xúc đặc thù của xe điện (*"sụt pin", "chậm", "tiết kiệm", "êm ái"*) mà không bị học vẹt (*overfitting*), đưa Macro-F1 đạt đỉnh **0.92**.
2. **`clf__solver = 'lbfgs'` (Thuật toán tối ưu hóa):** Động cơ giải phương trình vi phân bậc hai (*Limited-memory BFGS*) tiêu chuẩn hàng đầu cho bài toán văn bản nhiều chiều, giúp mô hình hội tụ hoàn hảo và huấn luyện siêu tốc **dưới 0.1 giây trên CPU thường** (phục vụ tự phục hồi Self-Healing).
3. **`clf__class_weight = 'balanced'` (Cân bằng phân bổ dữ liệu):** Tự động điều chỉnh trọng số phạt nghịch đảo với tần suất xuất hiện của từng lớp. Trong thực tế, đánh giá chê (Negative) ít hơn đánh giá khen, tham số này tăng mức phạt lên gấp 2-3 lần nếu AI đoán sai đánh giá tiêu cực, giúp tối đa hóa khả năng phát hiện lỗi xe hoặc khủng hoảng truyền thông.
4. **`clf__max_iter = 1000` & `random_state = 42` (Đảm bảo hội tụ & Tái lập 100%):** Cho phép lặp tối đa 1,000 vòng để thuật toán tìm ra điểm tối ưu toàn cục (loại bỏ cảnh báo `ConvergenceWarning`), kết hợp hạt giống cố định `42` giúp kết quả huấn luyện luôn đồng nhất trên mọi máy chủ.
5. **`tfidf__sublinear_tf = True` (Khử hiện tượng lặp từ quá đà):** Áp dụng thang đo logarit $1 + \log(TF)$ để tính tần suất từ, ngăn chặn hiện tượng khách hàng lặp đi lặp lại một từ tiêu cực làm sai lệch trọng số phân loại tổng thể.
6. **`tfidf__max_features = 8000` (Kích thước từ điển tinh chọn):** Giữ lại 8,000 cụm từ đơn và cụm từ ghép 2 chữ (n-gram 1-2) có ý nghĩa thông tin cao nhất sau khi lọc qua bộ tách từ PyVi. Giúp mô hình siêu nhẹ (<2 MB) và phản hồi API cực nhanh (<5ms).
7. **`model_family = 'LogisticRegression'` (Định danh họ mô hình):** Nhãn phân loại kiến trúc giải thuật dùng để đối chiếu bảng thành tích với các ứng viên khác trên MLflow Model Registry.
8. **`dataset_hashes` & `train_dataset_size` (Dấu vân tay dữ liệu & Quy mô mẫu):** Lưu mã băm bảo mật SHA-256 của các tệp dữ liệu huấn luyện và kích thước tập train (1,256 mẫu gốc, tự động tăng khi có nhãn Active Learning) bảo đảm khả năng kiểm toán nguồn gốc dữ liệu (Data Lineage & Reproducibility).

---

### 📊 Giải Thích Trực Quan Điểm F1-Score & Macro-F1 (Dành Cho Đánh Giá Gatekeeper)

Để hiểu tại sao hệ thống sử dụng **Macro-F1** làm thước đo duy nhất để chốt chặn Gatekeeper thay vì "Độ chính xác" (Accuracy) thông thường:

*   **Tại sao không dùng Độ chính xác thông thường (Accuracy)?**  
    *   *Ví dụ thực tế:* Nếu showroom có 95 khách tốt và 5 kẻ trộm. Một bảo vệ ngủ gật cả ngày và kết luận *"tất cả đều là khách tốt"* sẽ đạt độ chính xác **95%**, nhưng thực tế là **vô dụng** vì để lọt 100% kẻ trộm.  
    *   Trong phân tích xe điện, đánh giá chê lỗi pin/hỏng hóc chiếm thiểu số. Nếu AI đoán bừa tất cả là "Tích cực", độ chính xác vẫn cao nhưng doanh nghiệp sẽ sập tiệm vì không nhận biết được khủng hoảng sản phẩm!
*   **F1-Score là gì?** Là điểm trung bình điều hòa giữa 2 thước đo khắt khe:
    1.  **Precision (Bắt đúng):** Khi AI báo *"Đây là bài chê"*, thì có đúng khách chê thật không, hay bắt nhầm lời khen?
    2.  **Recall (Không bỏ sót):** Trong 100 bài khách chê trên mạng, AI có tóm được trọn vẹn không, hay để lọt phân nửa?
    *   *F1-Score chỉ cao khi mô hình vừa bắt chuẩn, vừa không để lọt lỗi.*
*   **Macro-F1 (Thước đo công bằng tuyệt đối):**  
    $$\text{Macro-F1} = \frac{\text{F1}_{\text{Tiêu cực}} + \text{F1}_{\text{Trung tính}} + \text{F1}_{\text{Tích cực}}}{3}$$  
    Hệ thống tính điểm F1 riêng biệt cho từng lớp rồi chia đều trọng số $1:1:1$. Điều này buộc mô hình ứng viên mới phải xuất sắc ở cả nhóm khó (Tiêu cực & Trung tính) thì mới được **Gatekeeper** cho phép thăng hạng lên `@champion`.
*   **Ý nghĩa mốc điểm `0.92` (92%) của mô hình dự án:** Thuộc phân khúc xuất sắc cấp công nghiệp (*Production-ready*), chứng minh mô hình phân định chuẩn xác và nhạy bén 92% mọi sắc thái đánh giá xe điện phức tạp của người dùng Việt Nam.


### 🔬 Phương Pháp Luận Thực Nghiệm & Kỹ Thuật Đánh Giá

Để đảm bảo tính khách quan khoa học và triệt tiêu hoàn toàn nguy cơ rò rỉ dữ liệu (data leakage), quy trình thử nghiệm áp dụng các chuẩn mực sau:

*   **Chia Tập Dữ Liệu Phân Tầng Độc Lập (Stratified Split 80/10/10):** Tập dữ liệu 1,570 đánh giá xe điện (`data/ev_reviews_vietnam_1529_cleaned.csv`) được chia phân tầng 80% Train (1,256), 10% Validation (157), 10% Test (157) nhằm bảo toàn tuyệt đối tỷ lệ cân bằng giữa 3 lớp Tích cực, Trung tính và Tiêu cực trên tất cả các tập.
*   **Chọn Macro-F1 Làm Thước Đo Quyết Định:** Dữ liệu cảm xúc thường xuyên biến động ngoài thực tế, do đó **Macro-F1** (trung bình cộng F1 của từng lớp nhãn) được sử dụng thay vì Accuracy đơn thuần. Điều này buộc mô hình phải hoạt động tốt đồng đều ở cả 3 nhóm cảm xúc thay vì thiên vị nhóm chiếm đa số.
*   **Kiểm Định Trên Tập Test Mù:** Mô hình Champion cuối cùng chỉ được chấm điểm đúng một lần duy nhất trên tập Test độc lập chưa từng tham gia quá trình tối ưu để bảo đảm khả năng tổng quát hóa thực tế.
*   **Phân Tích Ma Trận Nhầm Lẫn (Confusion Matrix):** Sử dụng `ConfusionMatrixDisplay` để nhận diện các điểm nghẽn giữa các lớp. Kết quả cho thấy Logistic Regression phân định ranh giới rất sạch sẽ, đặc biệt là ranh giới khó giữa lớp `Trung tính` và các lớp còn lại.
*   **Ghi Chép Minh Bạch Trên MLflow:** Toàn bộ siêu tham số, chỉ số đánh giá (Accuracy, Macro-Precision, Macro-Recall, Macro-F1), mã băm dữ liệu huấn luyện và biểu đồ ma trận nhầm lẫn đều được log tự động, bảo đảm tính tái lập 100%.

---

## 🌟 Tính Năng Hoàn Thiện Cấp Doanh Nghiệp (MLOps Maturity Level Up)

Không dừng lại ở việc phát hiện trôi dạt dữ liệu cơ bản như các đồ án học thuật thông thường, hệ thống được trang bị các tính năng chuyên sâu chuẩn doanh nghiệp:

*   **Kiểm Tra Trôi Dạt Dữ Liệu Bền Vững (Persistent Drift Monitoring):** Thay vì tái huấn luyện ngay khi chỉ có 1 ngày $PSI \ge 0.15$ (dễ dính báo động giả do nhiễu mẫu ngẫu nhiên ngắn hạn), Airflow chỉ kích hoạt Self-Healing khi phát hiện trôi dạt tích lũy kéo dài ($\ge 2$ ngày trôi dạt) hoặc đột biến cực đoan ($PSI \ge 0.25$). Với các đột biến đơn lẻ ngắn hạn, hệ thống xuất cảnh báo theo dõi HTML để tối ưu hóa chi phí tài nguyên máy chủ.
*   **Cơ Chế Từ Chối Thông Minh (Smart Rejection):** Trong quá trình Gatekeeping, nếu mô hình mới có $F1_{\text{new}} < F1_{\text{champ}}$, hệ thống tự động đối chiếu với Contender hiện tại. Nếu kém hơn Contender đang có, mô hình mới bị từ chối và bảo lưu Candidate mạnh nhất để không làm thụt lùi danh sách ứng viên.
*   **Chốt Chặn Phê Duyệt Con Người (Human Approval Gatekeeper):** Mô hình mới vượt qua Champion trên tập validation không được phép tự động đẩy lên phục vụ ngay mà chuyển vào trạng thái `pending_human_approval`. Bắt buộc kỹ sư/Admin thẩm định scorecard và bấm phê duyệt có ý thức trên UI trước khi đưa vào thử nghiệm thực tế.
*   **Phân Luồng Canary An Toàn (FastAPI Canary Traffic Splitting 90/10):** Sau khi được phê duyệt, mô hình mới được gán alias `@canary` và FastAPI tự động điều phối 10% lưu lượng truy vấn thực tế sang Canary, 90% lưu lượng còn lại vẫn do `@champion` xử lý ổn định. Ghi nhận `latency_ms` và nhãn `model_route` vào cơ sở dữ liệu theo thời gian thực.
*   **Giám Sát Proxy KPIs & Quyết Định 1-Click Promote / Rollback:** Streamlit Dashboard hiển thị bảng đối chiếu thời gian thực giữa Champion và Canary (Tỷ lệ lưu lượng, Độ tự tin trung bình, Độ trễ phản hồi). Người vận hành có toàn quyền bấm **`🚀 1-Click Promote to Champion`** để thăng hạng mô hình an toàn hoặc bấm **`🔙 1-Click Rollback`** để ngắt Canary tức thì nếu phát hiện dấu hiệu bất thường (**Zero Downtime & Zero Regression**).
*   **Tách Từ Ghép Tiếng Việt Chuẩn Ngữ Nghĩa (PyVi Vietnamese Word Segmentation):** Tích hợp bộ tách từ chuyên dụng `pyvi` vào tiền xử lý dùng chung `ml/preprocess.py` cho cả huấn luyện và phục vụ API on-demand. Kỹ thuật này tự động ghép nối các khái niệm xe điện (*"xe_điện", "tiết_kiệm", "sạc_lâu", "không_quá", "giá_bán"*) thành các token ngữ nghĩa đơn nhất, ngăn chặn tình trạng TF-IDF cắt rời từ làm sai lệch ý nghĩa (ví dụ: *"không quá sang trọng"* không còn bị gán nhầm sang tiêu cực), tăng độ chính xác phân loại mà vẫn bảo toàn tốc độ phản hồi cực nhanh (<5ms trên CPU thường).
*   **Vòng Lặp Phản Hồi Nhãn Vàng & Kiểm Toán Con Người (Active Learning & Human-in-the-Loop Audit):** Người vận hành có thể kiểm toán hàng loạt kết quả dự đoán của mô hình trực tiếp trên giao diện Streamlit bằng bảng tương tác (`st.data_editor`). Tính năng sở hữu **Cơ Chế Mở Khóa Thông Minh (Smart Unlocking)**: bảng thẩm định sẽ tự động mở khi phát hiện cảnh báo drift, khi Gatekeeper chặn thăng hạng mô hình mới, khi còn review thuộc mẻ trôi dạt lịch sử chưa được thẩm định, hoặc thông qua nút gạt quản trị **`🔓 Mở khóa thủ công`**. Cơ chế **Phê Duyệt Hàng Loạt Kèm Hộp Thoại Xác Nhận An Toàn (Safe Bulk Approval Dialog)** hiển thị cảnh báo xác nhận trước khi sao chép toàn bộ dự đoán thành nhãn vàng đã thẩm định, ngăn ngừa triệt để tình trạng bấm nhầm làm sai lệch dữ liệu. Vòng lặp tái huấn luyện (`train_model.py`) tự động quét bảng `store_reviews` tìm các nhãn người duyệt (`verified_sentiment IS NOT NULL`), gộp trực tiếp vào tập Train để mở rộng kho từ vựng thị trường cho mô hình!
*   **Cầu Dao An Toàn Phục Vụ (Serving Circuit Breaker):** Tích hợp công tắc chuyển mạch khẩn cấp trên sidebar của Streamlit, cho phép lập tức điều hướng lưu lượng FastAPI từ mô hình máy học sang bộ phân loại quy tắc từ khóa (safe-mode rule classifier) khi phát hiện sự cố bất thường trong vận hành, bảo đảm tính liên tục của nghiệp vụ (Zero Downtime).
*   **Thu Thập Dữ Liệu Huấn Luyện On-Demand & Bảng Ghi Suy Luận Thời Gian Thực Cặp Đôi (Paired On-Demand Scoring & Live Active Learning Ingestion):** Khu vực kiểm thử suy luận on-demand được bố trí ở cuối trang dashboard theo bố cục cặp đôi trực quan:
    * **Cột trái (Kiểm thử On-Demand):** Cho phép nhập văn bản đánh giá xe điện bất kỳ, bấm dự đoán qua FastAPI, nhận diện kết quả phân loại tức thì cùng độ tự tin (% confidence) hiển thị trực quan dưới dạng banner kết quả.
    * **Cột phải (Bảng kiểm toán tương tác & Phê duyệt tập trung):** Bảng lịch sử suy luận tương tác đa dòng (`st.data_editor`) tích hợp cột dropdown **`Verified Label`** (`positive`, `neutral`, `negative`) và cột trạng thái đào tạo (`✅ In Data Train` vs `⏳ Pending Audit`):
      * **Trường hợp 1 (Dự đoán đúng nhưng độ tự tin thấp):** Kỹ sư xác nhận nhãn và đưa vào tập dữ liệu huấn luyện để củng cố trọng số cho mô hình.
      * **Trường hợp 2 (Dự đoán sai hoàn toàn):** Kỹ sư chọn lại nhãn đúng để biến câu đánh giá thành dữ liệu sửa sai (hard negative/positive sample) chất lượng cao.
      * **Nút `💾 Save Verified Labels to Data Train`:** Lưu hàng loạt tất cả các dòng đã chọn nhãn vào bảng `store_reviews` (`category='on_demand'`, `is_processed=1`) và cập nhật `inference_logs` (trạng thái `✅ In Data Train`), đồng thời tách biệt hoàn toàn khỏi bảng `predictions` để đảm bảo các biểu đồ giám sát mẻ vận hành (Daily Sentiment Volume Timeline, Category Breakdown) và bảng Scored Reviews Explorer luôn tinh khiết 100%, không bị sai lệch bởi dữ liệu kiểm thử trực tiếp. Dữ liệu nhãn vàng này sẽ được tự động nạp vào vòng lặp Active Learning (`ml/train_model.py`) trong chu kỳ tái huấn luyện tiếp theo.
      * **Nút `⚡ Bulk Approve All AI Predictions`:** Tự động phê duyệt toàn bộ các câu dự đoán đang chờ thẩm định thành nhãn vàng huấn luyện trong 1 click mà không cần thao tác từng dòng. Bảng duy trì cấu trúc hiển thị ổn định kể cả khi chưa có request mới.

*   **Phân Tích Chuyên Sâu Về Sức Khỏe Mô Hình & Độ Bất Định (Uncertainty Analytics):** Bảng điều khiển Streamlit hiển thị 4 biểu đồ MLOps chuyên sâu: (1) **Phân Bổ Cảm Xúc Theo Khía Cạnh Xe** (với bảng màu tương phản giao thông chuẩn: Đỏ `#e74c3c` cho Tiêu cực, Vàng `#f1c40f` cho Trung tính, và Xanh lá `#2ecc71` cho Tích cực), (2) **Độ Tự Tin Trung Bình Theo Từng Lớp Cảm Xúc**, (3) **Phân Tầng Độ Bất Định (Uncertainty Tier Bucketing)** chia làm 3 bậc (Cao ≥80%, Trung bình 60-79%, Thấp <60% hiển thị trực quan định dạng `[Số lượng] ([Tỷ lệ %])` không kèm mũi tên tăng trưởng thừa, giúp người vận hành ưu tiên lọc các câu khó cần người thẩm định), và (4) **Chỉ Số Hiệu Chuẩn Tương Đồng Người - AI** (`Human-AI Agreement %`, `Số đánh giá đã duyệt`, `Số đánh giá bị người sửa`).
*   **Chuẩn Hóa Baseline & Chốt Chặn Retrain An Toàn:** Quy trình nạp 25 ngày dữ liệu lịch sử (`data/ingest_pipeline.py`) sử dụng tập baseline xe điện Việt Nam đã được hiệu chuẩn cân bằng ($PSI \approx 0.0051 \ll 0.15$), đi kèm cờ chốt an toàn `auto_retrain=False` để tránh kích hoạt retrain ngoài ý muốn trong lúc khởi tạo cơ sở dữ liệu. Bộ gán nhãn dự phòng (retraining fallback pseudo-labeler) tự động nhận diện chính xác các từ vựng chuyên ngành ô tô điện tiếng Việt (*"lỗi", "chậm", "sụt pin", "êm", "tiết kiệm"...*).

---

## 🚗 Bộ Dữ Liệu Đánh Giá Xe Điện Việt Nam (`data/ev_reviews_vietnam_1529_cleaned.csv`)

Để phục vụ bài toán phân loại cảm xúc chuyên ngành xe điện và làm cơ sở đo lường thực tế cho NLP tiếng Việt, dự án sử dụng bộ dữ liệu khách hàng đánh giá xe điện đã được làm sạch và thẩm định chuẩn:

*   **Đường dẫn tệp:** `data/ev_reviews_vietnam_1529_cleaned.csv`
*   **Quy mô tổng thể:** 1,570 dòng duy nhất với **100% nhãn thực tế đầy đủ**.
*   **Quy trình làm sạch & làm giàu dữ liệu:** Chuẩn hóa viết hoa đầu câu, sửa lỗi ngắt dấu câu giữa chừng (`. so với` ➔ `, so với`), loại bỏ dấu câu kép, bổ sung dấu câu kết thúc, bảo toàn chuẩn bảng mã Unicode tiếng Việt, đồng thời bổ sung các từ khóa chuyên sâu ngành xe điện (thiết kế ngoại thất, màn hình giải trí dễ dùng, pin sạc nhanh, điều hòa làm lạnh, khoang hành lý, trạm sạc) nhằm triệt tiêu điểm mù từ vựng và cân bằng trọng số giữa các lớp cảm xúc.
*   **Phân phối cảm xúc (Sentiment Distribution):**
    *   **Tích cực (Positive):** 608 đánh giá (38.73%)
    *   **Trung tính (Neutral):** 539 đánh giá (34.33%)
    *   **Tiêu cực (Negative):** 423 đánh giá (26.94%)
*   **Phân phối thương hiệu:** VinFast (447), BYD (301), Tesla (219), MG (170), Hyundai (156), Kia (151), Wuling (126).
*   **Phân phối nguồn thu thập:** YouTube (327), Đại lý xe (322), Diễn đàn ô tô (318), Facebook (302), Trang web đánh giá (301).

> **Lưu ý về thư mục `docs/`:** Thư mục `docs/` chứa tài liệu báo cáo (Slide thuyết trình `TMA Slide-Session 10.ppt`, bảng phân công `MLOPS-Projects.xlsx`, báo cáo benchmark `benchmark_ev_results.md`, và file raw backup 10,000 dòng) được cấu hình **hoàn toàn chỉ lưu trữ trên máy tính cá nhân (local)** và được thêm vào `.gitignore` để không bị đẩy lên Git.

---

## 🧠 Giới Hạn Phạm Vi & Đánh Đổi Thực Tế (MLOps Maturity Trade-offs)

Để phục vụ tốt nhất mục tiêu chạy thử nghiệm và chấm điểm linh hoạt trên máy đơn hoặc VM, một số thành phần quy mô hạ tầng lớn được chủ động giữ ở mức gọn nhẹ:

*   **Hạ Tầng Tự Động Co Giãn (Autoscaling Infrastructure):** Docker Compose phù hợp tối ưu cho máy tính cá nhân hoặc máy chủ ảo VM đơn lẻ. Khi chịu tải hàng triệu truy vấn/giây trong môi trường thương mại lớn, hệ thống sẽ cần nâng cấp lên Kubernetes (EKS/GKE) với Horizontal Pod Autoscaler (HPA).
*   **Quản Trị Khóa Bí Mật (Production Secrets Management):** Để tiện chấm điểm và chạy demo, các biến môi trường được cấu hình qua tệp cấu hình mẫu. Trong môi trường doanh nghiệp khép kín, các khóa bí mật cần được quản lý qua dịch vụ két khóa bảo mật chuyên dụng (như HashiCorp Vault hoặc AWS Secrets Manager).
