from textblob import TextBlob
import json
import pandas as pd



def load_json(file_path):

    with open(file_path) as json_file:
        data = json.load(json_file)
        return data



def main():
    print("nlp example")
    #data = load_json("../data/AlertDCio.json")
    data = load_json("../data/AlertDCio_google_geo.json")

    #print(data)



# blob = TextBlob(text)
# blob.tags           # [('The', 'DT'), ('titular', 'JJ'),
#                     #  ('threat', 'NN'), ('of', 'IN'), ...]

# blob.noun_phrases   # WordList(['titular threat', 'blob',
#                     #            'ultimate movie monster',
#                     #            'amoeba-like mass', ...])

# for sentence in blob.sentences:
#     print(sentence.sentiment.polarity)

    sent_dataframe = {}
    sent_dataframe["tweet"] = []
    sent_dataframe["lat"] = []
    sent_dataframe["lng"] = []
    sent_dataframe["sentiment"] = []


    for item in data:
        full_text = item["tweet"]
        lat = item["google_geo"]["lat"]
        lng = item["google_geo"]["lng"]
        print(full_text)

        blob = TextBlob(full_text)

        # TODO do some type of filttering


        # TODO custom model or key words is better?
        # nltk, huggingface, spacy?

        # TODO get address from tweets

        sent_score = 0
        for sentence in blob.sentences:
            print(sentence.sentiment.polarity)
            sent_score += sentence.sentiment.polarity
        

        sent_dataframe["tweet"].append(full_text)
        sent_dataframe["lat"].append(lat)
        sent_dataframe["lng"].append(lng)
        sent_dataframe["sentiment"].append(sent_score)

        

        
        print("\n")

    df = pd.DataFrame(sent_dataframe)
    df = pd.DataFrame.from_dict(sent_dataframe)
    df.to_csv("../data/nlp_example.csv")




if __name__ == "__main__":
    main()