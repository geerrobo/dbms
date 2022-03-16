# DOA README

## Requirements
* [python 3.10.x](https://www.python.org/downloads/) or higher
* [postgresql 14.x](https://www.postgresql.org/download/) or higher

## Setting Up
### Project
* clone project
> $ git clone https://github.com/geerrobo/dbms.git
* or
> $ git clone git@github.com:geerrobo/dbms.git
* use virtualenv or install packages globally
* pip install -r requirements.txt
### Database
* example dataset
    * [DVD Rental](https://www.postgresqltutorial.com/postgresql-sample-database/)
    * [Articles from russian site habr.com](https://www.kaggle.com/leadness/habr-posts)
* create table in database
> $ python manage.py migrate

## Run Project
### Django
> $ python manage.py runserver
