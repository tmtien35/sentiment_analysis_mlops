"""
Standalone SMTP Email Test Script
=================================
Runs an immediate test of the Gmail SMTP alert pipeline without triggering drift or Airflow.
Usage:
    python ml/test_email_smtp.py
"""
import os
import sys

# Ensure project root is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


def load_dotenv_config():
    """Load key-value pairs from .env file into os.environ if not already defined."""
    candidate_paths = [
        os.path.join(project_root, ".env"),
        os.path.join(os.getcwd(), ".env"),
        ".env"
    ]
    for p in candidate_paths:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            k, v = k.strip(), v.strip()
                            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                                v = v[1:-1]
                            if k not in os.environ:
                                os.environ[k] = v
                break
            except Exception:
                pass


def send_drift_email(subject: str, html_body: str, ds: str = None) -> bool:
    """Send real-time alert email via Gmail SMTP with dual-port fallback (587 STARTTLS -> 465 SSL)."""
    load_dotenv_config()
    sender_email = os.environ.get("SMTP_SENDER", "").strip()
    sender_password = os.environ.get("SMTP_PASSWORD", "").strip()
    recipient_email = os.environ.get("SMTP_RECIPIENT", "").strip() or sender_email

    if not sender_password or not sender_email:
        print("ℹ️  [EMAIL ALERT] Real email sending skipped (SMTP_SENDER or SMTP_PASSWORD not configured).")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg.attach(MIMEText(html_body, "html"))

    print(f"📧 [EMAIL ALERT] Attempting to deliver alert email from '{sender_email}' to '{recipient_email}'...")

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print(f"✅ [EMAIL ALERT] Alert email successfully delivered to '{recipient_email}' via Gmail Port 587 (STARTTLS)!")
        return True
    except smtplib.SMTPAuthenticationError as auth_err:
        print(f"❌ [EMAIL AUTH ERROR] Gmail rejected credentials: {auth_err}")
        return False
    except Exception as e587:
        print(f"⚠️ [EMAIL ALERT] Port 587 failed ({e587}). Auto-attempting Port 465 (SSL fallback)...")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print(f"✅ [EMAIL ALERT] Alert email successfully delivered to '{recipient_email}' via Gmail Port 465 (SSL)!")
        return True
    except Exception as e465:
        print(f"❌ [EMAIL ALERT] Both Port 587 and Port 465 failed: {e465}")
        return False


def main():
    print("=" * 65)
    print("📧 MLOps Platform - SMTP Email Delivery Diagnostic Test")
    print("=" * 65)

    load_dotenv_config()

    sender = os.environ.get("SMTP_SENDER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()
    recipient = os.environ.get("SMTP_RECIPIENT", sender).strip() or sender

    print(f"📁 Working Directory : {os.getcwd()}")
    print(f"📬 Sender Email       : {sender if sender else '(NOT SET ❌)'}")
    print(f"📥 Recipient Email    : {recipient if recipient else '(NOT SET ❌)'}")
    
    if password:
        masked = password[:2] + "*" * (len(password) - 4) + password[-2:] if len(password) >= 6 else "***"
        print(f"🔑 App Password       : {masked} ({len(password)} chars) ✅")
    else:
        print("🔑 App Password       : (NOT SET ❌)")

    print("-" * 65)

    if not sender or not password:
        print("\n❌ CẤU HÌNH EMAIL CHƯA ĐẦY ĐỦ!")
        print("\n👉 CÁC BƯỚC ĐỂ NHẬN ĐƯỢC EMAIL TRONG HÒM THƯ (1 PHÚT):")
        print("1. Tạo file '.env' ở thư mục gốc (hoặc copy từ .env.example):")
        print("   cp .env.example .env")
        print("2. Mở file '.env' và điền:")
        print("   SMTP_SENDER=email_cua_ban@gmail.com")
        print("   SMTP_PASSWORD=16_chu_cai_app_password")
        print("   SMTP_RECIPIENT=email_nhan_canh_bao@gmail.com")
        print("\n3. Cách lấy App Password 16 chữ cái từ Google:")
        print("   - Bật Xác minh 2 bước: https://myaccount.google.com/security")
        print("   - Tạo mật khẩu ứng dụng: https://myaccount.google.com/apppasswords")
        print("   - Copy 16 chữ cái (không tính khoảng trắng) dán vào SMTP_PASSWORD.")
        print("\n4. Chạy lại lệnh này: python ml/test_email_smtp.py\n")
        sys.exit(1)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    test_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 520px; border: 1px solid #e0e0e0; border-radius: 8px; padding: 20px; background-color: #ffffff;">
        <div style="text-align: center; border-bottom: 2px solid #27ae60; padding-bottom: 12px; margin-bottom: 16px;">
            <h2 style="color: #27ae60; margin: 0; font-size: 22px;">✅ SMTP EMAIL ALERT TEST PASSED</h2>
            <p style="color: #7f8c8d; font-size: 13px; margin: 4px 0 0 0;">Amazon Review Sentiment MLOps Pipeline</p>
        </div>
        <p style="font-size: 14px; color: #333333; line-height: 1.5;">
            Chúc mừng! Hệ thống cảnh báo tự động qua Email của bạn đã được kết nối thành công tới máy chủ <b>Gmail SMTP</b>.
        </p>
        <div style="background-color: #f8f9fa; border-left: 4px solid #27ae60; padding: 12px; margin: 16px 0; border-radius: 4px;">
            <p style="margin: 4px 0; font-size: 13px;"><b>Thời gian kiểm tra:</b> {now_str}</p>
            <p style="margin: 4px 0; font-size: 13px;"><b>Tài khoản gửi:</b> {sender}</p>
            <p style="margin: 4px 0; font-size: 13px;"><b>Hòm thư nhận:</b> {recipient}</p>
            <p style="margin: 4px 0; font-size: 13px;"><b>Trạng thái:</b> Sẵn sàng kích hoạt khi phát hiện Data Drift (PSI &gt; 0.15)!</p>
        </div>
        <div style="text-align: center; margin-top: 20px;">
            <a href="http://localhost:8501" style="background-color: #2980b9; color: #ffffff; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 14px; display: inline-block;">
                Mở Streamlit Dashboard
            </a>
        </div>
    </div>
    """

    print(f"🚀 Đang gửi email kiểm tra tới '{recipient}'...")
    success = send_drift_email(
        subject=f"✅ [MLOps Test] Kiểm tra kết nối Email Cảnh báo ({now_str})",
        html_body=test_html
    )

    if success:
        print("\n🎉 THÀNH CÔNG! Vui lòng mở hòm thư của bạn để kiểm tra email đến (có thể kiểm tra cả mục Spam/Thư rác).")
        print("=" * 65)
        sys.exit(0)
    else:
        print("\n❌ GỬI EMAIL THẤT BẠI. Vui lòng kiểm tra lại thông tin cấu hình trong .env.")
        print("=" * 65)
        sys.exit(1)


if __name__ == "__main__":
    main()
