from textblob import TextBlob
import json




def load_json(file_path):

    with open(file_path) as json_file:
        data = json.load(json_file)
        return data



def main():
    print("nlp example")
    data = load_json("../data/AlertDCio.json")

    #print(data)



# blob = TextBlob(text)
# blob.tags           # [('The', 'DT'), ('titular', 'JJ'),
#                     #  ('threat', 'NN'), ('of', 'IN'), ...]

# blob.noun_phrases   # WordList(['titular threat', 'blob',
#                     #            'ultimate movie monster',
#                     #            'amoeba-like mass', ...])

# for sentence in blob.sentences:
#     print(sentence.sentiment.polarity)

    for item in data:
        full_text = item["full_text"]
        print(full_text)

        blob = TextBlob(full_text)

        # TODO do some type of filttering


        # TODO custom model or key words is better?
        # nltk, huggingface, spacy?

        # TODO get address from tweets

        for sentence in blob.sentences:
            print(sentence.sentiment.polarity)
        
        print("\n")



if __name__ == "__main__":
    main()