import os
import sys
import subprocess
import time

def main():
    print("==================================================================")
    print("🛍️  MLOps Capstone: Launching Local Serving Stack")
    print("==================================================================")
    
    # 1. Run automated Pytest first
    print("\n[Step 1/3] Running automated quality assurance tests...")
    env = os.environ.copy()
    # Add root directory to python path
    env["PYTHONPATH"] = os.getcwd()
    
    test_proc = subprocess.run(
        [sys.executable, "-m", "pytest", "ml/test_pipeline.py"],
        env=env
    )
    
    if test_proc.returncode != 0:
        print("\n❌ Quality tests failed! Aborting server startup.")
        sys.exit(1)
        
    print("✅ Quality tests passed! Code is verified.")

    # 2. Spawn servers concurrently in the background
    print("\n[Step 2/3] Spawning server processes in background...")
    
    # Spawn FastAPI
    print(" - Booting real-time FastAPI server on port 8000...")
    fastapi_cmd = [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "8000"]
    fastapi_proc = subprocess.Popen(
        fastapi_cmd,
        env=env,
        stdout=subprocess.DEVNULL,  # Keep console pristine
        stderr=subprocess.DEVNULL
    )
    
    # Spawn Streamlit
    print(" - Booting Streamlit analytics dashboard on port 8501...")
    st_cmd = [sys.executable, "-m", "streamlit", "run", "dashboard/app.py", "--server.port", "8501", "--server.address", "127.0.0.1"]
    st_proc = subprocess.Popen(
        st_cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Spawn MLflow UI
    print(" - Booting MLflow tracking server on port 5000...")
    mlflow_cmd = [sys.executable, "-m", "mlflow", "ui", "--port", "5000", "--host", "127.0.0.1", "--backend-store-uri", "sqlite:///mlflow.db"]
    mlflow_proc = subprocess.Popen(
        mlflow_cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # Wait for servers to initialize
    time.sleep(3.5)
    
    # 3. Serving is live
    print("\n[Step 3/3] Serving stack is fully live and healthy!")
    print("==================================================================")
    print("👉 ACCESS STREAMLIT DASHBOARD:  http://localhost:8501")
    print("👉 ACCESS FASTAPI SWAGGER DOCS: http://localhost:8000/docs")
    print("👉 ACCESS MLFLOW TRACKING UI:   http://localhost:5000")
    print("==================================================================")
    print("\n🛑 TO STOP ALL PROCESSES (Single Action):")
    print("   Simply press [Ctrl + C] in this terminal window!")
    print("==================================================================")
    
    try:
        # Keep orchestrator running to maintain child processes
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 [Ctrl + C] Intercepted! Shutting down all servers...")
        
        # Kill the FastAPI server
        print(" - Terminating FastAPI server...")
        fastapi_proc.terminate()
        
        # Kill the Streamlit server
        print(" - Terminating Streamlit server...")
        st_proc.terminate()

        # Kill the MLflow server
        print(" - Terminating MLflow server...")
        mlflow_proc.terminate()
        
        # Wait for clean terminations to prevent orphaned background ports
        fastapi_proc.wait()
        st_proc.wait()
        mlflow_proc.wait()
        
        print("\n✅ All processes successfully shut down! Ports 5000, 8000 & 8501 are clean.")
        print("Goodbye!")

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
