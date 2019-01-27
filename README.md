# Coding Challenge Solution - Pyspark

If you already have Spark installed, those challenges use `findspark` module, available in the `requirements.txt` file

## dictionary.py

This job will read through the whole `dataset` folder and create an index of unique words with an associated ID. 
Resulting dictionary will be saved as `parquet` file.

## index.py

This job will create a dataframe of the list of words and the document ID where they appear. 
The list won't be of unique words. Repetitions need to appear to understand the full list of documents where this word is located. 

After the full read is done, we will join the previous dictionary with this new dataframe, to obtain a list of `wordId` vs `docId`. 

When we have that list, we will collect all the `docId` into a list, so we can finally have unique entries for words, together with the full list of docs where they appear. 

