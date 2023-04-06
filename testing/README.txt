Testing is a important part of a robust ML pipeline

The first goal is to identify what needs to be tested

A resource (credit to Jason for the idea):
https://medium.com/analytics-vidhya/testing-ml-code-how-scikit-learn-does-it-97e45180e834

An example of testing implemented in the context of ensemble learning, is found at:
https://github.com/scikit-learn/scikit-learn/tree/main/sklearn/ensemble/tests

Upon discussion (credit to Greeshma), we have decided to focus on implementing unit testing in two locations
- for important individual functions
- for the overall machine learning pipeline - here we would take a sample data set (in .csv form) and run it through the pipeline - it would be undesirable to have to read from the database every time

Here's a resource:
https://towardsdatascience.com/testing-machine-learning-pipelines-22e59d7b5b56