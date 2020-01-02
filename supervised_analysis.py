## SETUP
import requests
import string
import pandas as pd
import matplotlib.pyplot as plt

from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.en import English

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics

# AUTHENTICATION
# key and token from Trello - both needed for authentication
with open('/Users/alec/Python/KEYS/trello.env', 'r') as f:
    myKey = f.readline().rstrip('\n')
    myToken = f.readline().rstrip('\n')

# IDs of the Trello boards
backlog = '5541fead2c739608a8898ebe'
archive = '5c41dfb19fd465437c5e3ec7'
in_play = '58fddaeebcc004044e9b133e'
everything = [backlog, archive, in_play]

#%%

# get all the cards from the Trello archive 
def get_data(board, call, fields = []):
    """Make an Trello API call for a 'board' object. 
    Use 'call' to specify what you want - eg 'cards' or 'labels'.
    Fields should be a list of string field names - eg ['id']
    List of API calls at https://developers.trello.com/reference/#boardsboardid-1
    """
    url = f"https://api.trello.com/1/boards/{board}/{call}/"
    payload = {
            'key': myKey,
            'token': myToken,
            'fields': fields
            }
    response = requests.get(url, params = payload)
    return response.json()

# Get all the cards (also labels, just to check they're as expected)
cards = []
labels = []
for board in everything: 
    cards.extend(get_data(board, 'cards', ['url', 'name', 'desc', 'labels']))
    labels.extend(get_data(board, 'labels', ['name', 'color'])) 

# Set of section labels - not used, just to check the right 9 are there
section_labels = {l['name'] for l in labels if l['color'] == 'sky'}

#%%
# Prepare data - just top level section for now
# This isn't actually useful because we could easily predict section 
# from the card title. But it's a test of the supervised approach. 

# Add top level section to cards - might be faster to do this inside the 
# dataframe, but with this little data it's fine
for c in cards:
    c['section'] = ''
    for l in c['labels']:
        if l['color'] == 'sky':
            c['section'] = l['name'].lower()
            break # only allow one per card, even if there are more

# bung it into a dataframe, drop spare columns and rows with no section label
df = pd.DataFrame.from_dict(cards)
df.drop(['id', 'labels'], axis = 1, inplace = True)
df = df[df['section'] != '']

# check and chart the distribution of sections
df.groupby('section')['name'].count().plot.bar(ylim=0)
plt.show()

# split the dataframe into a test set and a training set
# later introduce cross-validation
name_train, name_test, text_train, text_test, label_train, label_test = train_test_split(
        df[['name', 'url']], df['desc'], df['section'], 
        test_size=0.3,
        random_state = 1) # use random_state to set the same seed each time

#%%
# Code from here onwards based on an excellent tutorial: 
# https://www.dataquest.io/blog/tutorial-text-classification-in-python-using-spacy/
# This cell is the main one to tinker with to improve results

#SET UP CLEANER CLASS
class predictors(TransformerMixin):
    """
    """
    def transform(self, X, **transform_params):
        """Basic text cleaning - I'll want to do more here
        At the moment it removes trailing spaces, lowercases text,
        and removes the signature. I could also remove all URLs,
        dates, emails and numbers (esp classification numbers)
        and the Scotland signature
        """
        result = [text.split('Page URL: /')[0] \
         .split('------------------------------')[0] \
         .strip().lower() for text in X]
        return result   
        #return [text.strip().lower() for text in X 
    
    def fit(self, X, y = None, **fit_params):
        """
        """
        return self
    
    def get_params(self, deep=True):
        """
        """
        return {}

#SET VECTOR AND TOKENIZER
# add extra punctuation to the built-in list, via James and Ian!
punctuation = string.punctuation + '““””•…–—'
parser = English()

def spacy_tokenizer(sentence):
    """This is a custom function from the tutorial - I need to dig into this
    I want to try it with the Spacy built-in Tokenizer first
    But basically it turns words into their basic lemmas
    """
    tokens = parser(sentence)
    #-PRON- is a Spacy lemma that means 'any pronoun' - 
    # it treats them all equally - I need to look at this more
    tokens = [word.lemma_.lower().strip() if word.lemma_ != "-PRON-" 
              else word.lower_ for word in tokens]
    tokens = [word for word in tokens 
              if word not in STOP_WORDS 
              and word not in punctuation]
    return tokens

# 2 different options for turning the lists of words into vectors
# once they're vectors (matrices of numbers) you can do calculations on them
# I'll need to make a decent way to assess them before choosing
bow_vector = CountVectorizer(tokenizer = spacy_tokenizer, ngram_range = (1,1))
tfidf_vector = TfidfVectorizer(tokenizer = spacy_tokenizer, ngram_range = (1,1))

#SET CLASSIFIER
classifier = LogisticRegression(class_weight = 'balanced',
                                solver = 'liblinear',
                                multi_class = 'ovr')

#%%
from sklearn.feature_extraction.text import TfidfTransformer


# Run the cleaner, vector and classifier into a pipeline
pipe = Pipeline([('cleaner', predictors()),
                 ('vectorizer', bow_vector),
                 ('tfidf', TfidfTransformer()),
                 ('classifier', classifier)])

# push the training data into the pipeline to get a model
pipe.fit(text_train, label_train)

# apply the model to the test data and see how well it did
predicted = pipe.predict(text_test)
print("Accuracy:", metrics.accuracy_score(label_test, predicted))

# testing scikit learn's cross validation - seems easy
from sklearn.model_selection import cross_val_score
scores = cross_val_score(pipe, text_train, label_train, cv=5)
print("Cross-validation accuracy:", sum(scores)/5)

#%%

# Make results table so I can export if I want
results = pd.DataFrame({
        'Ticket': name_test['name'],
        'Url': name_test['url'],
        'Actual topic': label_test, 
        'Predicted topic': predicted})
    
# compare distribution between predicted and actual categories
actual = results.groupby('Actual topic')['Actual topic'].count()
modelled = results.groupby('Predicted topic')['Predicted topic'].count()

df = pd.DataFrame({'Actual topic counts': actual,
                   'Predicted topic counts': modelled},
        index=section_labels)

ax = df.plot.bar(rot=0)

#results.to_csv('results.csv', index = False)