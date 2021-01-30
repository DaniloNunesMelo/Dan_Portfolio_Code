# Hello world

```python
pip install -U tensorflow keras opencv-python
pip install tensorflow-cpu
pip3 install imageai --upgrade
```

```bash
> python brain.py
## Error
> python brain.py
2021-01-29 09:24:44.255065: I tensorflow/compiler/jit/xla_cpu_device.cc:41] Not creating XLA devices, tf_xla_enable_xla_devices not set
2021-01-29 09:24:44.255756: I tensorflow/core/platform/cpu_feature_guard.cc:142] This TensorFlow binary is optimized with oneAPI Deep Neural Network Library (oneDNN) to use the following CPU instructions in performance-critical operations:  AVX2 FMA
To enable them in other operations, rebuild TensorFlow with the appropriate compiler flags.
brain.py:10: MatplotlibDeprecationWarning: '.predictImage()' has been deprecated! Please use 'classifyImage()' instead.
  predictions, probabilities = prediction.predictImage(os.path.join(execution_path, "giraffe.jpg"), result_count=5 )
2021-01-29 09:24:45.503352: I tensorflow/compiler/mlir/mlir_graph_optimization_pass.cc:116] None of the MLIR optimization passes are enabled (registered 2)
2021-01-29 09:24:45.503808: I tensorflow/core/platform/profile_utils/cpu_utils.cc:112] CPU Frequency: 1991995000 Hz
Downloading data from https://storage.googleapis.com/download.tensorflow.org/data/imagenet_class_index.json
```

## Result

- leopard  :  1.841738075017929
- lynx  :  1.8286250531673431
- cheetah  :  1.8222730606794357
- jaguar  :  1.6499241814017296
- impala  :  0.8933961391448975


Currently supported shells are:
  - bash
  - cmd.exe
  - fish
  - tcsh
  - xonsh
  - zsh
  - powershell