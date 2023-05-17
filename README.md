![Ironhack logo](https://i.imgur.com/1QgrNNw.png)

**Description:**

In this project, you will practice:

- Extraction
- SQL
- NLTK (sentiment analysis)
- Tableau



0. Think of some questions you want to find a response to: the average rating for a given genre of movies on IMDB and their description, the average sentiment of a twitter username across time, the reviews for given categories of products are greater than other categories....whatever comes to mind. Make some questions before starting the process

1. Find data. This can be: hotel reviews, tweets, lines per character in a TV show, dialogues from a book, whatsapp/telegram/tinder conversations, etc. Whatever, really!

2. The data can be scrapped or obtained through an API, but it doesn't need to be. You can also look for csv files.

3. Clean and transform your data.

4. Load the data you obtain into SQL. Do this through Python if possible. Do you want to make it relational? Do you have things like: users, chats & messages? characters, episodes & seasons? Does it make sense to make it relational? Just one table? Define a DB diagram that makes sense. You may encounter some errors while inserting, as text can be tricky since it takes different formats. You will either have to: not do anything (data will be formatted well enough) or transform it using Python or outside of Python.

5. Now you have a clean database. Do queries and subqueries if you can.

6. Use another source of data so you can have more detailed analysis. You can just relate it semantically. This will help you generate Tableau visualizations.

7. Generate a Tableau dashboard with the insights you found.



# STEPS
1. Extract data
2. Transform it through Python
3. Add another source of info if needed
4. Load that data into SQL through Python & SQL

5. Try to answer as many questions as you can by running SQL queries through Python: aggregations, averages, comparisons, filtered data, etc
6. Export the result of those queries as csv files

7. On Tableau, create stories/dashboards.
8. Include your csv files from the queries & the original csv without it being queried too so you can have more visualizations

## Project Goals

- ETL
- EDA
- Analysis of text
- Visualization using Tableau

## Deliverables
  
- Python files
  - Modularization
  - Documented & organized code: docstrings, comments, etc

- SQL file
  - In this file you should gather all the SQL questions that describe your data.
    - Description of central tendency of your variables per groups
    - Frequencies
    - Percentages of a given category
    - Group things with conditions 
      - Same things from another table?
    - Be creative! What questions would you have about this data if you were the owner of a company? Write as many meaningful queries as you can!
  
- Link to Tableau dashboard


## Links - NLP & Text Sentiment Analysisç

- [nltk](https://www.nltk.org/)
- [Towards data science - Srntiment analysis using nltk](https://towardsdatascience.com/basic-binary-sentiment-analysis-using-nltk-c94ba17ae386)
- [Digital ocean: performing sentiment analysis](https://www.digitalocean.com/community/tutorials/how-to-perform-sentiment-analysis-in-python-3-using-the-natural-language-toolkit-nltk)
