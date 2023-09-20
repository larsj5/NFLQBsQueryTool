#CS205 Warmup Project
#Lars Jensen, Mathias Tefre, Andrew Snell, Cael Christian
from __future__ import division
from ast import keyword
from multiprocessing import connection
#import readline
import sqlite3
from typing import Type
import pandas as pd
from pathlib import Path
import os
import time

#from symbol import tfpdef

def csv_is_valid(filename):
    """
    Given a string filename of .csv, the function determines 
    if the if the file exists and if it is empty. If 
    the file is valid, return true.
    Args:
        filename (string): filename of csv.

    Returns:
        bool: If the csv file is valid.
    """    
    # check if csv file exists
    if (os.path.isfile(filename)):
        # check if csv is empty
        if (os.path.getsize(filename) == 0):
            print(str(filename) + " file is empty! Cannot load empty dataset into a database.")
            return False
        else:
            return True
    else:
        print(str(filename) + " file does not exist!")
        return False

"""
Function creates database file if it does not exist 
already and writes/overwrites two sqllite tables 
from twoCSV files.

Returns:
    bool: If the table was successfully initialized.
"""    
def initialize_table():
    #hardcode file names
    #load everything into sql
    """ Create table 'NFLTeams' database """

    # check if csv files exist and are NOT empty
    csv_is_valid("NFLTeams.csv")
    csv_is_valid("NFLQBs.csv")

    # if database file has not been loaded/created, create DB file
    if not (os.path.isfile("NFL.db")):
        # create a database file if one does not exist already
        Path('NFL.db').touch()
        
    # check if database file is empty
    if (os.path.getsize("NFL.db") != 0):
        print("\nReinitializing database file.")
    
    try:
        # create connections to NFL.db
        # cursor is used to load data.
        # if db file doesn't exist, it is created
        connection = sqlite3.connect('NFL.db')
        cursor = connection.cursor()
        
        # load datasets into a Pandas DataFrame
        teams = pd.read_csv('NFLTeams.csv')
        quarterbacks = pd.read_csv('NFLQBs.csv')
        
        # write the data to a sqlite table
        teams.to_sql('Teams', connection, if_exists='replace', index = False)
        quarterbacks.to_sql('Quarterbacks', connection, if_exists='replace', index = False)
        
        # save changes
        connection.commit()
        
        print("Datasets have been loaded into \"NFL.db\".\n")
        time.sleep(1)
        return True
    
    except BaseException:
        print("Connection could not be made to database file. Dataset was not loaded.")
        return False
    
    # close connections
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
            
        
    """_summary_: Function prints results of sql statement when given a list of keywords

    Args:
        query (list): query is a validated list of key words that can be used to execute sql statements
        cursor (sqlite3 cursor object): instance to invoke methods that execute SQLite statements

    Returns:
        void: returns nothing
    """
def do_query(query, cursor):
    if query[0] == "how" and query[1] == "many":
        tableName = query[2]
        res = cursor.execute("SELECT COUNT(*) FROM " + tableName)
        print(res.fetchone()[0])

    elif query[0] == "average":
        columnName = query[2]
        res = cursor.execute("SELECT AVG(" + columnName + ") FROM quarterbacks")
        print(f"{res.fetchone()[0]:.2f}")
    

    elif query[0] == "conference":
        conference = query[1]
        if query[2] == "division":
            division = query[3]
            res = cursor.execute("SELECT name FROM teams WHERE conference = '" + conference + "' AND division = '" + division + "'")

        else:
            res = cursor.execute("SELECT name FROM teams WHERE conference = '" + conference + "'")
        if len(res.fetchall()) > 0:
            for x in res.fetchall():
                print(x[0])
        else:
            print("Conference not found.")

    elif query[0] == "team":
        teamName = query[1]
        teamID = cursor.execute("SELECT teamID FROM teams WHERE name = '" + teamName + "'")
        # try to fetch teamID, if error team is not in database
        try:
            teamID = teamID.fetchone()[0]
        except TypeError:
            print("Sorry, that team isn't in our database")
            return 0

        if (query[2] == "division" or query[2] == "conference" or query[2] == "stadium"):
            columnName = query[2]
            res = cursor.execute("SELECT " + columnName + " FROM teams WHERE name = '" + teamName + "'")
            print(res.fetchone()[0])

        elif query[2] == "quarterbacks":
            res = cursor.execute("SELECT name FROM quarterbacks WHERE teamID = '" + str(teamID) + "'")
            if (len(res.fetchall())) > 0:
                for x in res.fetchall():
                    print(x[0])
            else:
                print("Team was not found.")
        
        elif query[2] == "starting":
            if ("age" in query or "jersey" in query or "mvps" in query):
                columnName = query[4]
                if query[4] == "jersey":
                    columnName = query[5]
                res = cursor.execute("SELECT " + columnName + " FROM quarterbacks WHERE teamID = '" + str(teamID) + "'")
                    
            else:
                res = cursor.execute("SELECT name FROM quarterbacks WHERE teamID = " + str(teamID) + " AND starter = TRUE")
    
            print(res.fetchone()[0])

    elif query[0] == "quarterback":
        playerName = query[1]
        playerID = cursor.execute("SELECT playerID FROM Quarterbacks WHERE name = '" + playerName + "'") 

        if (query[2] == "age" or query[2] == "mvps" or query[2] == "starter" or query[2] == "jersey"):
            columnName = query[2]
            if query[2] == "jersey":
                columnName = query[3]
            res = cursor.execute("SELECT " + columnName + " FROM quarterbacks WHERE name = '" + playerName + "'")


        elif query[2] == "team":
            teamID = cursor.execute("SELECT teamID FROM Quarterbacks WHERE name = '" + playerName + "'")
            try:
                teamID = teamID.fetchone()[0]
            except TypeError:
                print("Sorry that quarterback isn't in our database")
                return 0
            
            res = cursor.execute("SELECT name FROM Teams WHERE teamID = '" + str(teamID) + "'")
        try:
            if columnName == "starter":
                print(bool(res.fetchone()[0]))
            else:
                print(res.fetchone()[0])
        except TypeError:
            print("Sorry that quarterback isn't in our database")


# functions parse_input takes the query from the user and determines
# if it is a valid query or not given the rules of the language. It has
# two return values, a bool to signify if it was a valid query or not,
# and a list of all the keywords. There is one special case: load data
# will return True but an empty list of keywords, while all other valid 
# queries will return true and an list of all the keywords, while invalid 
# queries return false and an empty list. 
def parse_input(user_text):
    no_queries = []
    valid_query = []
    team_name = ""
    qb_name = ""

    user_text = user_text.lower()
    keywords = user_text.split()

    if (len(keywords) == 2 and keywords[0] == "load" and keywords[1] == "data"):
        initialize_table()
        return True, valid_query 
    elif (len(keywords) < 3):
        return False, no_queries
    elif (keywords[0] == "team"):
        #add the team name 
        valid_query.append("team")
        if (keywords[1].startswith("\"")):
            team_name += keywords[1].lstrip("\"")
            i = 2
            while (i <= len(keywords) - 1 and (not keywords[i].endswith("\""))):
                team_name += " " + keywords[i]
                i += 1
            if (i == len(keywords)):
                return False, no_queries
            team_name += " " + keywords[i].rstrip("\"")
            i += 1 #now this is the index of the next keyword
            valid_query.append(team_name)

            #check for all 3 keyword combos with team name
            if (len(keywords) == i + 1 and keywords[i] == "quarterbacks"):
                valid_query.append("quarterbacks")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "division"):
                valid_query.append("division")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "conference"):
                valid_query.append("conference")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "stadium"):
                valid_query.append("stadium")
                return True, valid_query

            #for starting quarterback queries, first check if there are enough keywords in query so we don't index out of bounds
            if (len(keywords) < i + 2):
                return False, no_queries
            elif (keywords[i] == "starting" and keywords[i+1] == "quarterback"):
                valid_query.append("starting")
                valid_query.append("quarterback")
                if (len(keywords) == i + 2):
                    return True, valid_query
                elif (len(keywords) == i + 3 and keywords[i + 2] == "age"):
                    valid_query.append("age")
                    return True, valid_query
                elif (len(keywords) == i + 3 and keywords[i + 2] == "mvps"):
                    valid_query.append("mvps")
                    return True, valid_query
                elif (len(keywords) == i + 4 and keywords[i + 2] == "jersey" and keywords[i + 3 == "number"]):
                    valid_query.append("jersey")
                    valid_query.append("number")
                    return True, valid_query
                else:
                    return False, no_queries
        else:
            return False, no_queries

    elif (keywords[0] == "conference"):
        valid_query.append("conference")
        valid_query.append(keywords[1])
        if (keywords[2] == "teams" and len(keywords) == 3):
            valid_query.append("teams")
            return True, valid_query
        elif (len(keywords) == 5 and keywords[2] == "division" and keywords[4] == "teams"):
            valid_query.append("division")
            valid_query.append(keywords[3])
            valid_query.append("teams")
            return True, valid_query
        else:
            return False, no_queries

        
    elif (keywords[0] == "quarterback"):
        #add the qb name 
        valid_query.append("quarterback")
        if (keywords[1].startswith("\"")):
            qb_name += keywords[1].lstrip("\"")
            i = 2
            while (i <= len(keywords) - 1 and (not keywords[i].endswith("\""))):
                qb_name += " " + keywords[i]
                i += 1
            if (i == len(keywords)):
                return False, no_queries
            qb_name += " " + keywords[i].rstrip("\"")
            i += 1 #now this is the index of the next keyword
            valid_query.append(qb_name)
            if (len(keywords) == i + 1 and keywords[i] == "age"):
                valid_query.append("age")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "mvps"):
                valid_query.append("mvps")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "starter"):
                valid_query.append("starter")
                return True, valid_query
            elif (len(keywords) == i + 1 and keywords[i] == "team"):
                valid_query.append("team")
                return True, valid_query
            elif (len(keywords) == i + 2 and keywords[i] == "jersey" and keywords[i + 1] == "number"):
                valid_query.append("jersey")
                valid_query.append("number")
                return True, valid_query
            else:
                return False, no_queries
        else:
            return False, no_queries
    
    elif (keywords[0] == "average" and keywords[1] == "quarterback"):
        valid_query.append("average")
        valid_query.append("quarterback")
        if (keywords[2] == "age" and len(keywords) == 3):
            valid_query.append("age")
            return True, valid_query
        elif (keywords[2] == "mvps" and len(keywords) == 3):
            valid_query.append("mvps")
            return True, valid_query
        else:
            return False, no_queries

    elif (keywords[0] == "how" and keywords[1] == "many" and (keywords[2] == "teams" or keywords[2] == "quarterbacks")):
        valid_query.append("how")
        valid_query.append("many")
        valid_query.append("teams")
        return True, valid_query
    else:
        return False, no_queries
    
    return False, no_queries


def print_language_rules():
    print("\nThe rules of our language are as follows:")
    print("If you want to exit the program, type quit.")
    print("If you need help at any point with the language, type help.")

    print("\n - Queries must start with one of the following keywords: team, conference, quarterback, average, or how many")
    print(" - \"team\" must come before the team name (ex. team New York Giants), likewise for conference, quarterback, and division")
    print(" - A team name or quarterback name must be put in double quotes, as they are longer than one word")
    print(" - You can query for a team's quarterbacks, starting quarterback, division, stadium, or conference")
    print(" - You can query for a conference's teams or all teams in a division.")
    print("   example of the latter would be: conference AFC division West")
    print(" - You can query for a quarterbacks, age, mvps, if they're the starter, their team, or their jersey number")
    print(" - You can query for the average quarterback age or mvps across all quarterbacks")
    print(" - You can query for how many teams or how many quarterbacks\n")

def print_valid_keywords():
    print(" The complete list of valid keywords is: team(s), \"<team name>\", quarterback(s), \"<quarterback name>\", starting quarterback, age, jersey number,", end = "")
    print(" mvps, division, stadium, conference, <conference name>, <division name>, starter, average, and how many")

def help():
    print("Here are some examples of valid queries:")
    print("team \"Denver Broncos\" quarterbacks")
    print("team \"New England Patriots\" starting quarterback age")
    print("conference AFC division West teams")
    print("\nAnd some invalid queries with common mistakes:")
    print("\"Mac Jones\" jersey number (needs to start with quarterback)")
    print("team New York Jets stadium (need quotes around team name)")
    print("quarterback \"Joe Burrow\" jersey (...number)\n")
    print_valid_keywords()

def main():
    
     # while database is uninitialized
    if (not os.path.isfile("NFL.db")):
        print("It looks like this is your first time using our CLI, please enter the one-time command \"load data\" to initialize the database. " +
          "This is only necessary the first time running the app. " + 
          "You will not be able to enter queries until you do so. " +
          "To quit, enter \"quit\".")
                
        
        user_input = input("\n>  ")
        # while user doesn't quit or load data, loop
        while ((user_input.strip() != "quit") & (user_input.strip() != "load data")):
            query_keywords = []
            query_valid = False
            query_valid, query_keywords = parse_input(user_input)
            user_input = input("> ")
            
        # if user doesn't quit, load data
        if user_input.strip() == "quit":
            quit()
        else:
            initialize_table()
            
    #print out a welcome message and how the query language works
    print("Welcome to the NFL Teams and QB Database Explorer!")
    # time.sleep(2)
    print_language_rules()
    # time.sleep(2) 
    print_valid_keywords()
    
    try:
        # create connections to NFL.db
        # cursor is used to load data.
        # if db file doesn't exist, it is created
        connection = sqlite3.connect('NFL.db')
        cursor = connection.cursor()     
    
        #then start looping until the user types quit
            #each time user enters a command, parse it
            #if valid, call do query function and print out return value
            #if not valid, return error message and print out rules of parser again
            #ask for user input again
        user_input = input("> ")
        while (user_input != "quit"):
            if user_input == "help":
                help()
            else:
                query_keywords = []
                query_valid = False
                query_valid, query_keywords = parse_input(user_input)
                if query_valid == True and len(query_keywords) == 0:
                    print()
                elif query_valid:
                    do_query(query_keywords, cursor)
                else:
                    print("Invalid Query")
            user_input = input("> ")
        #if clause for if query valid is false
        #need to have a if clause for if query valid is true and first element is 'load data successful'
        #if clause for query valid true
        
    except sqlite3.DatabaseError as err:
        print("A sqlite3.DatabaseError exception occurred: " + str(err))
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


        

#initialize_table()  # Run initialize_table function first time to create the database
#do_query(cursor,query)  # View all data stored in the database

main()
