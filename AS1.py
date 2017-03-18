# CMPUT 291 MINI-PROJECT 1
# 
# Members:
# Mercy Woldmariam
# Andrea Whittaker      1386927

import sqlite3
import string
import re

def loginScreen(c, conn):
    loggedIn = False
    while loggedIn == False:
        
        print("Login")
        username = raw_input('What is your Username? Cannot be longer than 8 characters ')
        password = raw_input('What is your Password? Cannot be longer than 30 characters ')
        
        if re.match("^[A-Za-z0-9_]*$", username) and re.match("^[A-Za-z0-9_]*$", password):
            
            c.execute("SELECT * FROM staff WHERE login = ? and password = ?;", (username, password))
            row = c.fetchone()
            
        if row == None:
            
            options = raw_input("Input S to Sign Up or T to try logging in again! ")
            
            if options == "S" or options == "s": 
                
                staff_id = raw_input('What is your Staff ID? Cannot be longer than 5 characters ')
                role = raw_input('What is your role? Enter D for Doctor, N for Nurse or A for Adminstration ')
                name = raw_input('What is your name? Cannot be longer than 15 characters ')
                
                c.execute(" INSERT INTO staff VALUES (?, ?, ?, ?, ?); ", (staff_id, role, name, username, password))
                conn.commit()
                c.execute( ' SELECT login, password FROM staff; ')
                row = c.fetchone()
                print(row)
                
                if len(staff_id) <= 5 and role in ('D', 'N', 'A') and len(name) <= 15 and len(username) <= 8 and len(password) <= 30:
                    loggedIn = True
    return role

# -------------------------- start care staff --------------------------

def firstTask(c, conn):
    
    print("-"*10)
    not_valid = True
    
    # "list all charts in the system ordered by adate"
    c.execute("SELECT * FROM charts ORDER BY adate;")
    
    # "indicating if it is opened or closed"
    charts = c.fetchall()
    for chart in charts:
        if chart["edate"] == "Null":
            print(chart["chart_id"], " closed")
        else:
            print(chart["chart_id"], " open")
    
    # "the user should be given the option to select a chart"
    print("-"*10)
    while not_valid:
        
        #changed input to raw_input
        chartId = raw_input("Enter a chart id (0 to quit): ")
        
        if chartId == "0":
            return     
            
        c.execute("SELECT chart_id FROM charts;")
        ids = c.fetchall()
        
        if chartId not in ids:
            print("Invalid chart id. Please try again")
            
        else:
            not_valid = False

    # "all entries associated with that chart must be listed, 
    # ordered by date of entries"
    # implement order. . .
    c.executescript('''
    SELECT * 
    FROM symptoms
    WHERE chart_id=?
    ORDER BY obs_date
    
    SELECT *
    FROM diagnoses
    WHERE chart_id=?
    ORDER BY ddate
    
    SELECT *
    FROM medications
    WHERE chart_id=?
    ORDER BY mdate;
    ''', chartId)

    return

def secondTask(c, conn, staffId):
    
    print("-"*10)
    not_valid = True
    
    # "for a given patient"
    Hcno = raw_input("Enter the patient's hcno: ")
    
    # "and an open chart of the patient"
    while not_valid:
        chartId = raw_input("Enter the patient's chart id: ")
        c.execute("SELECT edate FROM charts WHERE chart_id=?;", chartId)
        Edate = c.fetchone()
        if Edate == "Null":
            print("That chart is closed. Please try again.")
        else:
            not_valid = False
    
    Symptom = raw_input("Enter a symptom name: ")
    
    # "add an entry for symptoms"
    c.execute("INSERT INTO symptoms VALUES (?,?,?,datetime('localtime'),?);", Hcno, chartId, staffId, Symptom)
    conn.commit()
    
    return

# -------------------------- end care staff --------------------------
# -------------------------- start doctor --------------------------

def getDoctorTask():
    
    not_valid = True
    
    print("-"*10)
    print("Welcome, doctor. Here are the tasks you can perform:")
    
    print("1: List all charts in the system.")
    print("2: Add a symptom to a patient's chart.")
    print("3: Add a diagnosis to a patient's chart.")
    print("4: Add a medication to a patient's chart.")    

    while not_valid:
        
        key = raw_input("Enter the number that corresponds to the task (0 to logout): ")
        
        if key not in ('0', '1', '2', '3', '4'):
            print("Invalid input. Please try again.")
        
        else:
            not_valid = False
    
    return key        


def dThirdTask(c, conn, staffId):
    
    print("-"*10)
    not_valid = True
    
    # "for a given patient"
    Hcno = raw_input("Enter the patient's hcno: ")
    
    # "and an open chart of the patient"
    while not_valid:
        chartId = raw_input("Enter the patient's chart id: ")
        c.execute("SELECT edate FROM charts WHERE chart_id=?;", chartId)
        Edate = c.fetchone()
        if Edate == "Null":
            print("That chart is closed. Please try again.")
        else:
            not_valid = False
    
    Diagnosis = raw_input("Enter a diagnosis: ")
    
    # "add an entry for symptoms"
    c.execute("INSERT INTO symptoms VALUES (?,?,?,datetime('localtime'),?);", Hcno, chartId, staffId, Diagnosis)
    conn.commit()
    
    return    

def dFourthTask(c, conn, staffId):
    
    print("-"*10)
    not_open = True
    amount_not_valid = True
    no_allergy_check = True
    
    # "for a given patient"
    Hcno = raw_input("Enter the patient's hcno: ")
    
    # "and an open chart of the patient"
    while not_open:
        chartId = raw_input("Enter the patient's chart id: ")
        c.execute("SELECT edate FROM charts WHERE chart_id=?;", chartId)
        Edate = c.fetchone()
        if Edate == "Null":
            print("That chart is closed. Please try again.")
        else:
            not_open = False 
    
    # check (2)
    while no_allergy_check:
        
        drugName = raw_input("Enter a drug name: ")  
        
        c.execute("SELECT * FROM inferredallergies ia, patients p WHERE hcno = ? AND (? = ia.alg OR ? = ai.canbe_alg);", Hcno, drugName)        
        Alg = c.fetchall()
        
        if drugName in Alg[alg]:
            print("WARNING: this patient reported being allergic to " + drugName + ".")
            
        elif drugName in Alg[canbe_alg]:
            print("WARNING: this patient could be allergic to " + drugName + " because they reported an allergy to " + Alg[alg] + ".")
            
        else:
            no_allergy_check = False
    
    # check (1)
    while amount_not_valid:
        
        Amount = int(raw_input("Enter a daily amount: "))
        c.execute("SELECT d.sug_amount FROM dosage d, patients p WHERE p.hcno=? AND p.age_group = d.age_group AND d.drug_name=?;", Hcno, drugName)
        sugAmount = c.fetchone()
        
        # "if prescribed amount for the patient is larger than the recommended amount"
        if sugAmount < Amount:
            print("WARNING: " + sugAmount + " is the suggested amount of " 
                  + drugName + " for a patient of " + Hcno + "'s age group.")
            
            confirm = raw_input("Would you like to confirm your prescription? (Y/N): ")
            
            if confirm == 'Y' or confirm == 'y':
                print("Prescription confirmed.")
                amount_not_valid = False

    startMed = raw_input("Enter the start date (YYYY-MM-DD HH:MM:SS): ")
    endMed = raw_input("Enter the end date (YYYY-MM-DD HH:MM:SS): ")
    
    # "add an entry for medications"
    c.execute("INSERT INTO medications VALUES (?,?,?,datetime('localtime'),?,?,?,?);", Hcno, chartId, staffId, startMed, endMed, Amount, drugName)    
    conn.commit()
    
    return

# -------------------------- end doctor --------------------------
# -------------------------- start nurse --------------------------

def getNurseTasks():
    
    not_valid = True
        
    print("-"*10)
    print("Welcome, nurse. Here are the tasks you can perform:")
    
    print("1: List all charts in the system.")
    print("2: Add a symptom to a patient's chart.")
    print("3: Create a chart for a patient.")
    print("4: Close a patient's chart.")            

    while not_valid:
        
        key = raw_input("Enter the number that corresponds to the task (0 to logout): ")
        
        if key not in ('0', '1', '2', '3', '4'):
            print("Invalid input. Please try again.")
        
        else:
            not_valid = False
    
    return key    
    
    
def nThirdTask():
    
    # implement . . .
    
    return

def nFourthTask():
    
    return
    
# -------------------------- end nurse --------------------------
# -------------------------- start admin --------------------------

# -------------------------- end admin --------------------------
# -------------------------- start staff --------------------------
   
def staffLogout():
    
    print("-"*10)
    print("Goodbye!")
    print("-"*10)
    
    return
    
# -------------------------- end staff --------------------------
# -------------------------- start main --------------------------
 
def main():
    import os.path 
    conn = sqlite3.connect('memory')
    c = conn.cursor()
    c.execute(' PRAGMA foreign_keys=ON; ')
    createTables = open('p1-tables.sql', 'r')
    script = createTables.read()
    c.executescript(script)
    conn.commit()
    createTables.close()
    
    user = loginScreen(c, conn)
    
    key = 'z'
    if user == 'D':
        
        while key != '0':

            key = getDoctorTask()            
            
            if key == '1':
                firstTask(c, conn)
            elif key == '2':
                Hcno = raw_input("Enter the patient's hcno: ")
                secondTask(c, conn, staffId)
            elif key == '3':  
                dThirdTask(c, conn)
            elif key == '4':    
                # needs to be made
                dFourthTask(c, conn)
        
        staffLogout()
        loginScreen(c, conn)

    # still have to implement . . .
    elif user == 'N':
        
        while key != '0':
            
            key = getNurseTask()
            
            if key == '1':
                firstTask(c, conn)
            elif key == '2':
                Hcno = raw_input("Enter the patient's hcno: ")
                secondTask(c, conn, staffId)
            elif key == '3':  
                nThirdTask(c, conn)
            elif key == '4':    
                nFourthTask(c, conn)
        
        staffLogout()
        loginScreen(c, conn)            

    #implement all functions . . .
    elif user == 'A':
        
        while key != '0':
            
            key = get_admin_task()
            
            if key == '1':
                aFirstTask(c, conn)
            elif key == '2':
                aSecondTask(c, conn)
            elif key == '3':  
                aThirdTask(c, conn)
            elif key == '4':    
                aFourthTask(c, conn)
        
        staffLogout()
        loginScreen(c, conn)              
   

main()