import os
import subprocess
import sys


def start_litellm(config: str = "litellm_config.yaml", port: int = 4000) -> None:
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config)
    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    litellm_bin = os.path.join(os.path.dirname(sys.executable), "litellm")
    cmd = [litellm_bin, "--config", config_path, "--port", str(port)]
    print(f"Starting LiteLLM proxy on http://localhost:{port}")
    print(f"Using config: {config_path}")
    print("Press Ctrl+C to stop.\n")

    proc = subprocess.Popen(cmd)
    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nStopping LiteLLM proxy...")
        proc.terminate()
        proc.wait()
        print("Stopped.")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
    start_litellm(port=port)
