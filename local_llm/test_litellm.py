import anthropic

client = anthropic.Anthropic(
    base_url="http://localhost:4000",   # LiteLLM proxy
    api_key="sk-local-key",            # matches master_key in litellm_config.yaml
)

message = client.messages.create(
    model="qwen-openvino",             # matches model_name in litellm_config.yaml
    max_tokens=512,
    messages=[{"role": "user", "content": "Explain OpenVINO in one sentence."}],
)
print(message.content[0].text)
