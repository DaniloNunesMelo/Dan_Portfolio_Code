from imageai.Prediction import ImagePrediction
import os
execution_path=os.getcwd()

prediction = ImagePrediction()
prediction.setModelTypeAsMobileNetV2()
prediction.setModelPath(os.path.join(execution_path, "mobilenet_v2.h5"))
prediction.loadModel()

predictions, probabilities = prediction.predictImage(os.path.join(execution_path, "giraffe.jpg"), result_count=5 )
for eachPrediction, eachProbability in zip(predictions, probabilities):
    print(eachPrediction , " : " , eachProbability)
    
## Result

# ruffed_grouse  :  28.50576341152191
# prairie_chicken  :  10.893949121236801
# cheetah  :  10.37883311510086
# German_short-haired_pointer  :  7.698050141334534
# partridge  :  6.035150587558746    
    