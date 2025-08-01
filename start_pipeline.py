import subprocess
import time
import sys
import os

def run_command(cmd, shell=True):
    """Run command and return success status"""
    try:
        result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def main():
    print("🚀 Starting Fraud Detection Pipeline...")
    
    # Check if Docker is available
    success, _, _ = run_command("docker --version")
    if not success:
        print("❌ Docker not found. Please install Docker first.")
        return
    
    # Start Kafka with Docker Compose
    print("📦 Starting Kafka...")
    success, stdout, stderr = run_command("docker-compose up -d")
    if not success:
        print(f"❌ Failed to start Kafka: {stderr}")
        return
    
    print("⏳ Waiting for Kafka to be ready...")
    time.sleep(15)  # Give Kafka time to start
    
    # Create topics
    print("🔧 Setting up Kafka topics...")
    success, stdout, stderr = run_command("python setup_kafka.py")
    if success:
        print("✅ Topics created successfully")
    else:
        print(f"⚠️  Topic creation warning (may already exist): {stderr}")
    
    print("\n🎉 Pipeline is ready!")
    print("\nNext steps:")
    print("1. Run: python producer.py")
    print("2. Run: python fraud_detector.py") 
    print("3. Run: python dashboard.py (optional)")
    print("\nTo stop Kafka: docker-compose down")

if __name__ == "__main__":
    main()