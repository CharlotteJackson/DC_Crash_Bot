from sentiment_class import sentiment
sentimentInstance = sentiment()
sentimentLocal   = sentimentInstance.isPos("I like hats")
print('Positive' +  str( sentimentLocal['probs'][0])) 
print('Negative' +  str(sentimentLocal['probs'] [1]))

answer = sentimentInstance.askQuestion("I like hats", "what do you like ")
print(answer['best_span_str'] )