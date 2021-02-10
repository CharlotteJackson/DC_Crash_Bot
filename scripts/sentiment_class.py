# run sudo pip  install allennlp==1.0.0 allennlp-models==1.0.0       
# to install 
import os 
from allennlp.predictors.predictor import Predictor
import allennlp_models.rc
class sentiment:
    def __init__(self):
        self.predictor = Predictor.from_path("https://storage.googleapis.com/allennlp-public-models/bidaf-elmo-model-2020.03.19.tar.gz")
        self.predictorSent = Predictor.from_path("https://storage.googleapis.com/allennlp-public-models/basic_stanford_sentiment_treebank-2020.06.09.tar.gz")

    def askQuestion(self , tweet_text, question = "Who was hurt "):
        return self.predictor.predict(
        passage=tweet_text,
        question= question
        )
    
    def isPos(self,text):
        return self.predictorSent.predict(
        sentence= text
        )
       