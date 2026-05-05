# qwen2.5-7b-openvino — File Reference

This directory contains Qwen2.5-7B-Instruct exported to OpenVINO IR format, plus all
configuration files needed to serve it via OpenVINO Model Server (OVMS).

---

## Why two levels? Root vs `1/`

```
qwen2.5-7b-openvino/     ← OVMS reads graph.pbtxt and ovms_config.json from here
├── graph.pbtxt
├── ovms_config.json
├── <tokenizer + config files>   ← source copies live here
└── 1/                           ← OVMS GenAI reads the model from here
    ├── openvino_model.xml/.bin  ← actual model files
    ├── openvino_tokenizer.*
    ├── openvino_detokenizer.*
    └── <symlinks → ../…>        ← symlinks so GenAI finds config alongside the model
```

OVMS requires the model weights to be in a **versioned subdirectory** (`1/`, `2/`, …).
The GenAI pipeline also expects tokenizer config files in the same folder as the model,
so they are symlinked from the parent rather than duplicated.

---

## OVMS / MediaPipe files (root only)

### `graph.pbtxt`
MediaPipe graph definition — the core config that wires OVMS to the model.

**What it contains:**
- `device:` — which hardware to run on (`"GPU"`, `"NPU"`, `"CPU"`)
- `pipeline_type:` — inference mode (`LM_CB` for continuous batching on GPU/CPU; `LM` for static-shape on NPU)
- `models_path:` — path inside the container where OVMS finds `openvino_model.xml`
- `SyncSetInputStreamHandler` — **required** MediaPipe synchronization component; without it the LOOPBACK back-edge stalls and requests are silently dropped
- `max_num_batched_tokens`, `max_num_seqs`, `cache_size`, `dynamic_split_fuse` — throughput tuning

**What you edit here when switching devices.** See the main README for per-device values.

### `ovms_config.json`
Tells OVMS which pipelines to register at startup.

```json
{
  "model_config_list": [],
  "mediapipe_config_list": [
    { "name": "qwen", "graph_path": "/model/graph.pbtxt" }
  ]
}
```

- `name: "qwen"` is the pipeline name that appears in the OVMS REST URL
  (`POST /v3/chat/completions` with model `"qwen"`)
- `graph_path` is the path **inside the Docker container** (the `-v` mount maps
  the local directory to `/model`)
- `model_config_list` is empty because this is a GenAI/MediaPipe pipeline,
  not a classic IR model — OVMS discovers the weights via `graph.pbtxt`

---

## OpenVINO model files (`1/` only)

### `openvino_model.xml` (2.5 MB)
The model **graph** in OpenVINO Intermediate Representation (IR) format.
This is an XML description of every operation in the neural network —
layer types, shapes, connections, and quantization metadata.
It references `openvino_model.bin` for the actual weight data.

### `openvino_model.bin` (7.1 GB)
The model **weights** in binary format.
Contains all 7.6B parameters compressed as INT8 (`nncf int8_asym`, group_size=-1).
Despite `config.json` saying `bfloat16`, this file stores INT8 values with FP16 scales —
the model is dequantized to FP16 at runtime for each operation.
INT8 storage saves ~50% memory vs FP16 without significant quality loss.

### `openvino_tokenizer.xml` / `openvino_tokenizer.bin`
The **tokenizer** compiled into an OpenVINO model.
Instead of running Python HuggingFace tokenizer code, OVMS executes this as an
OpenVINO graph, which allows the tokenization step to run efficiently on the same
device pipeline. Converts raw text → token IDs.

### `openvino_detokenizer.xml` / `openvino_detokenizer.bin`
The **detokenizer** compiled into an OpenVINO model.
Converts token IDs → text. Handles BPE merges and special token stripping natively.
Like the tokenizer, this runs as an OpenVINO compute graph rather than Python code.

---

## Config / tokenizer files (root, symlinked into `1/`)

These files exist at the root level. The `1/` directory contains symlinks pointing
back to them (`1/config.json → ../config.json`, etc.) so OVMS GenAI can find them
next to the model files.

### `config.json`
HuggingFace model architecture config.
OVMS reads this to know the model structure: 28 layers, 28 attention heads,
3584 hidden size, 32768 max sequence length, 152064 vocabulary size.
Also consumed by `optimum-intel` when re-exporting the model.

### `generation_config.json`
Default generation hyperparameters loaded by OVMS at startup:

```json
{
  "eos_token_id": [151645],   ← <|im_end|> only — NOT 151643 (bos)
  "temperature": 0.7,
  "top_p": 0.8,
  "top_k": 20,
  "repetition_penalty": 1.05,
  "do_sample": true
}
```

**Critical:** `eos_token_id` must contain only `151645` (`<|im_end|>`).
If `151643` (`<|endoftext|>` / bos) is included, generation stops after the first
token because the bos token appears at position 0 of every output.

Parameters sent by the client (`temperature`, `max_tokens`, `stop`) override these
defaults per request.

### `openvino_config.json`
Metadata written by `optimum-intel` during export. Records:
- `optimum_version: "2.1.0"` — the exporter version used
- `transformers_version: "4.57.6"` — transformers version at export time
- `quantization_config` — NNCF quantization recipe (method, dataset, processor)

OVMS does not actively use this file at runtime; it is an audit trail for
reproducibility and re-export.

### `tokenizer.json` (not shown in ls — standard HF tokenizer)
Full HuggingFace tokenizer definition in JSON. Contains the BPE merge rules,
vocabulary, and normalizers. Used by `optimum-intel` during model export and
by client-side code that needs to count tokens before sending requests.

### `vocab.json`
Maps token strings → integer IDs (the vocabulary lookup table).
150,000+ entries for Qwen's multilingual BPE vocabulary. Referenced by
`tokenizer.json` and `openvino_tokenizer.xml`.

### `merges.txt`
BPE merge rules — the ordered list of byte-pair merges that define how raw text
is split into tokens. Qwen2.5 uses ~150K merge rules for its multilingual vocab.

### `tokenizer_config.json`
HuggingFace tokenizer metadata: chat template reference, special token IDs,
padding side, `add_bos_token: false` (important — no automatic BOS prepended).

### `added_tokens.json`
Maps special token strings → their integer IDs:

| Token | ID | Purpose |
|---|---|---|
| `<\|endoftext\|>` | 151643 | BOS / pad token |
| `<\|im_start\|>` | 151644 | Start of ChatML turn |
| `<\|im_end\|>` | 151645 | End of ChatML turn (EOS) |
| `<tool_call>` / `</tool_call>` | 151657/151658 | Function calling |
| `<\|fim_prefix\|>` / `<\|fim_suffix\|>` / `<\|fim_middle\|>` | 151659–151661 | Fill-in-the-middle (code) |
| `<\|vision_start\|>` … | 151652–151656 | Vision tokens (multimodal) |

### `special_tokens_map.json`
Maps abstract roles (`eos_token`, `pad_token`) to their token strings, with
formatting flags (lstrip, rstrip, normalized). Used by HuggingFace tokenizer
when running outside OVMS (e.g. client-side token counting).

### `chat_template.jinja`
Jinja2 template that formats a list of messages into the raw text prompt
the model was trained on. Implements Qwen's **ChatML** format:

```
<|im_start|>system
You are a helpful assistant.<|im_end|>
<|im_start|>user
Hello<|im_end|>
<|im_start|>assistant
```

Also handles tool calls, tool responses, and multi-turn history.
OVMS applies this template server-side when it receives a `/chat/completions`
request, so client code never needs to format prompts manually.

---

## File dependency map

```
Request arrives at OVMS
        │
        ▼
  ovms_config.json ──► graph.pbtxt
                              │
                    ┌─────────┼──────────────┐
                    ▼         ▼              ▼
            device: GPU   models_path    pipeline_type
                    │         │              │
                    │         ▼              ▼
                    │   openvino_model.xml   generation_config.json
                    │   openvino_model.bin   chat_template.jinja
                    │   openvino_tokenizer.* tokenizer_config.json
                    │   openvino_detokenizer.*
                    │         │
                    └─────────┘
                              │
                              ▼
                      Token generation
                      (temperature, top_p, eos from generation_config.json)
```
