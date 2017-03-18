# --------------------------------
# CMPUT 291 MINI-PROJECT 2
# Andrea Whittaker        1386927
# Mercy Woldmariam        1413892
# --------------------------------

import os.path
import sqlite3
import itertools

def readDatabase(conn, c):
    
    c.execute(" SELECT * FROM sqlite_master WHERE type = 'table'; ")
    tables = c.fetchall()
    
    listTables = []
    createTablesStatements = []
    index = 13

    FDS = {}
    
    for table in tables:
        listTables.append(table[2])
        
    insertValueLT = 0
    
    for table in listTables:
        insertValueString = str(table)
        listTables.remove(table)
        listTables.insert(insertValueLT, insertValueString)
        insertValueLT += 1
    
    for table in tables:
        createTablesStatements.append(table[4])

    insertValueCT = 0
        
    for table in createTablesStatements:
        insertValueString = str(table)
        createTablesStatements.remove(table)
        createTablesStatements.insert(insertValueCT, insertValueString)
        insertValueCT += 1
    
    RTableList = []
    FDsTableList = []
    
    index = 0 
    
    for table in listTables:
        currentTables = table.split("_")
        if len(currentTables) == 2:
            RTableList.append((table, createTablesStatements[index]))
        else: #len(currentTables) == 3
            FDsTableList.append((table, createTablesStatements[index]))       
        index += 1
    
    orderedRTableList = []    
    orderedFDsTableList = []
    
    lonely = []
    orderPairsList = []
    
    for table in RTableList:
        tableName = table[0].split("_")[-1]
        
        for table1 in FDsTableList:
            
            if table1[0].split("_")[-1] == tableName:
                orderedRTableList.append(table)
                orderedFDsTableList.append(table1)
                orderPairsList.append((table[0], table1[0]))
                
            else:
                lonely.append(table)
        
    
    shorter = 0
    L = ""
    
    if len(RTableList) > len(orderedFDsTableList):
        shorter =  len(orderedFDsTableList)
        L = "R"
    else:
        shorter = len(RTableList)
        L = "FD"
        
    numberSchemas = len(RTableList)
    
    
    for table in RTableList:
        if table not in orderedRTableList:
            orderedRTableList.append(table)  
            orderPairsList.append(table[0])
        
    for table in FDsTableList:
        if table not in orderedFDsTableList:
            orderedFDsTableList.append(table)
            orderPairsList.append(table[0])

    types = [[] for i in range(numberSchemas)]
    coloumns = ["" for i in range(numberSchemas)]
    
    newIndex = 0
    
    for table in orderedRTableList:
        stringweneed = "PRAGMA table_info(" + table[0] + ")"
        c.execute(stringweneed)
        columns = c.fetchall()


        indexColumns = 0
    
        coloumns[newIndex] = ""
        
        for table in columns:
            attributeString = str(table[1])
            datatypeString = str(table[2])
            
            
            coloumns[newIndex] += attributeString
            
            columnNameDataType = attributeString + " " + datatypeString
            
            types[newIndex].append(columnNameDataType)
            
        newIndex += 1

    
    key = ""
    value = ""
    
    FDs = [{} for i in range(len(orderedFDsTableList))]
    
    indexFuncD = 0
    indexDataV = 0
    
    dataValues = []
    
    for table in orderedRTableList:
        
        executeStatement = "SELECT * FROM " + table[0]

        c.execute(executeStatement)
        dataValue = c.fetchall()
        
        dataValues.append(dataValue)

    for table in orderedFDsTableList:

        executeStatement = "SELECT * FROM " + table[0]

    
        c.execute(executeStatement)                    
        dataFDs = c.fetchall() 
    
        #organize functional dependencies into the dictionary
        for FD in dataFDs:
        
            key = ""
            value = ""
        
            for attribute in FD[0]:
                if attribute.isalpha():
                    key += attribute.upper()
        
            key1 = str(key)
            key1.replace("u", "")
            key1.replace("'", "")
            
            for attribute in FD[1]:
                if attribute.isalpha():
                    value += attribute.upper()
            
            value1 = str(value)
            value1.replace("u", "")
            value1.replace("'", "")            
        
            FDs[indexFuncD][key1] = value1
    
        indexFuncD += 1
        
        

    print(types) 
    print(orderPairsList)
    print(coloumns)
    print(dataValues)
    print(FDs)
        
    return types, orderPairsList, coloumns, dataValues, FDs


def getTask():
    
    print " "
    print "I am your database normalization assistant! How can I help you?" 
    print "1. Synthesize a 3NF schema for the input table"
    print "2. Decompose the input table into BCNF"
    print "3. Create new tables for the normalized schemas"
    print "4. Fill the new data tables according to the decomposition"
    print "5. Compute the attribute closure of a set of attributes"
    print "6. Compare two sets of functional dependencies"
    print "7. Drop the Output tables from the database"
    task = raw_input("Please enter the number corresponding to the task (0 to exit): ")
    
    return task

# ---------------------- BCNF functions ---------------------- 

def BCNF(R, FDs):
    
    old_R = R[:]
    old_FDs = FDs.copy()
    bcnf_needed = True
    superkey = ''
    schema = []
    schemaR = []
    decomp = []
    schema.append(R)# [[A, B, C]]
    schema.append(FDs) # [[A, B, C], {A -> D, . . . }]
    decomp.append(schema) # [[[A, B, C], {A -> D, . . . }]]
    
    while bcnf_needed:
        
        bcnf_needed = False
        
        for S in decomp:
            
            schema = []

            # find the superkey of that schema
            superkey, closureS = findClosure(S[0], S[1], [])    
            
            if superkey == '':
                S.append(None)
            
            for X in S[1]:
                
                superkey, closureX = findClosure(S[0], S[1], [X])
                # find a key that violates BCNF
                if not set(closureX).issuperset(set(S[0])):
                    
                    bcnf_needed = True
                    
                    # set up the new schema FDs
                    s1FDs = {}
                    s1FDs[X] = S[1][X]
                    
                    schemaR = []
                    for a in X:
                        schemaR.append(a)
                        
                    for Y in s1FDs[X]:
                        schemaR.append(Y)
                    
                    # remove Y from the old schema
                    i = 0
                    while i < len(S[0]):
                        if S[0][i] in s1FDs[X]:
                            S[0].remove(S[0][i])
                            i -= 1
                        i += 1
                    
                    del S[1][X]
                    # remove the FD from the old schema's FDs
                    changed = True
                    while changed:
                        changed = False
                        for at in s1FDs[X]:
                            for key in S[1]:
                                if at in key:
                                    del S[1][key]
                                    changed = True
                                    break
                                elif at in S[1][key]:
                                    S[1][key] = S[1][key].replace(at, '')
                                    if len(S[1][key]) == 0:
                                        del S[1][key]
                                        changed = True
                                        break
                            
                    
                    superkey, closure = findClosure(schemaR, s1FDs, [])
                    # append the new schema to the decomposition
                    schema.append(schemaR)
                    schema.append(s1FDs)
                    if superkey == '':
                        schema.append([])
                    else:
                        schema.append(superkey)
                    decomp.append(schema)
                    
                    break
    
    #decomp = [[['A', 'D', 'E'], {'A' : 'DE'}, 'A'], [['A','B','C','F','G','H'], {'ABH' : 'C', 'BGH' : 'F', 'F' : 'AH', 'BH' : 'G'}, 'BGH']]
    #old_R = ['A','B','C','D','E','F','G','H']
    #old_FDs = {'ABH': 'C', 'A' : 'DE', 'BGH' : 'F', 'F' : 'ADH', 'BH' : 'GE'}
    
    # find out if it is dependency preserving
    new_FDs = {}
    for new_schema in decomp:
        new_FDs.update(new_schema[1])
        
    for old_key in old_FDs:
        dependency_preserving = False
        old_closure = []
        for old_x in old_key:
            old_closure.append(old_x)
        for old_y in old_FDs[old_key]:
            old_closure.append(old_y) 
            
        if old_key not in new_FDs:            
            # see if the original key and its dependencies can be derived
            for new_X in new_FDs:
                superkey, new_closure = findClosure(old_R, new_FDs, [new_X])
                if set(new_closure).issuperset(set(old_closure)):
                    dependency_preserving = True
                    break
                
        elif len(old_FDs[old_key]) > len(new_FDs[old_key]):
            for new_X in new_FDs:
                superkey, new_closure = findClosure(old_R, new_FDs, [new_X])
                if set(new_closure).issuperset(set(old_closure)):
                    dependency_preserving = True
                    break
        else:
            dependency_preserving = True
        
        if not dependency_preserving:
            print(" ")
            print("*** BCNF done -- decomposition is not dependency preserving ***")
            return decomp
    
    if dependency_preserving:
        print(" ")
        print("*** BCNF done -- decomposition is dependency preserving ***")
    else:
        print(" ")
        print("*** BCNF done -- decomposition is not dependency preserving ***")
    
    return decomp


def findClosure(R, FDs, user_data):
    
    closure = set()
    closure_list = []
    superkey = ''

    # find which X is the superkey of the relation
    for X in FDs:
        
        changed = True
        
        # add the attributes in X and Y to the closure set
        for attribute in X:
            closure.add(attribute)
        for Y in FDs[X]:
            closure.add(Y)

        # go through the Xs and see if we missed any
        while changed:
            changed = False
            for x in FDs:
                no_a = False
                for a in x:
                    if a not in closure:
                        no_a = True
                        break
                if not no_a:
                    for y in FDs[x]:
                        if y not in closure: # add the Ys that we missed                            
                            closure.add(y)
                            changed = True

        
        # if the user requested the closure of some attributes, print it
        if X in user_data:
            #print("The closure of " + X + " is " + ', '.join(closure))
            for item in closure:
                closure_list.append(item)
            return superkey, closure_list
    
        # if the closure is all attributes in the relation, it is a superkey
        if closure.issuperset(R) and user_data == []: 
            superkey = X
            for item in closure:
                closure_list.append(item)
            return superkey, closure_list
        
        closure.clear()
    
    return superkey, closure_list


# ---------------------- 3NF functions ----------------------

def thirdNF(R, FDs, superkey):

    cover = minimalCover(R, FDs)
    
    LHS = []
    LHSides = []
    
    for dependancy in cover:
        if dependancy not in LHS:
            L = set()
            L.add(dependancy[0])

            if L not in LHS:
                LHS.append(L)
                
    for dependancy in cover:
        if dependancy not in LHSides:
            L = set()
            L.add(dependancy[0])

            if L not in LHSides:
                LHSides.append(L)        
    
    relations = []
    
    RHS = len(LHS) * [set()]
    
    for dependancy in cover:
        for L in LHS:
            if dependancy[0] in L:
                L.add(dependancy)       
    setAttributes = []
    
    for item in LHS:
        setAttributes.append(list(item))
    
    relations = []
    for i in range(len(setAttributes)):
        relations.append(set())
    
    index = 0 
    
    for item in LHSides:
        LHSides.remove(item)
        LHSides.insert(index, list(item))
    
    while index < len(setAttributes):
        for L in setAttributes[index]:
            list1 = []
            list1.append(L)
            if len(L) > 1 and list1 not in LHSides:
                for M in L:
                    relations[index].add(M)
                    
        index += 1
    
    string = ""
    index = 0
    
    for set1 in relations:
        
        string = ""
        
        for i in set1:
            string += i
        
        relations.remove(set1)
        relations.insert(index, string)
        index += 1
    
    superkey, closure_list = findClosure(R, FDs, [])
    
    if list(superkey) not in LHSides and superkey != 'oops':
        superkeyRelational = set()
        superkeyRelational.add(superkey)
        LHS.append(superkeyRelational)
        relations.append(superkey)
    
    finalRelationalTables = []
     
    index = 0
    
    while index < len(LHS):
        currentRelational = []
        currentRelational.append(relations[index])
        currentFDs = set()
        
        if len(LHS[index]) > 1:
            for L in LHS[index]:
                #print(L)
                list1 = []
                list1.append(L)
                
                if len(L) > 1 and list1 not in LHSides:
                    currentFDs.add(L)
        
        finalRelationalTables.append([currentRelational, currentFDs])
        
        index += 1
    
    #print("These is your new decomposition")
    #print(finalRelationalTables)
    
    #index = 1
    #for item in finalRelationalTables:
        #print("R" + str(index) + " = " + item[0][0])
        
        #for fd in item[1]:
            #print("U" + str(index) + " = " + fd[0] + " -> " + fd[1])
            
        #index += 1
        
    LHSides.reverse()   
    if superkey != '':
        LHSides.append([superkey])
    
    print(" ")    
    print("*** 3NF Done ***")
    
    return(finalRelationalTables, LHSides)


def findClosure3Nf(R, FDs, user_data):
    
    closure = set()
    superkey = ''
    
    # first, deal with the attributes in user data that have no dependencies
    if user_data != []:
        for attribute in user_data:
            if attribute not in FDs:
                
                for attribute1 in attribute:
                    closure.add(attribute1)
                    
                    if attribute1 in FDs:
                        if len(FDs[attribute1]) == 1:
                            closure.add(FDs[attribute1])
                        
                        else: #len(FDs[attribute1]) >= 1
                            for dependency in FDs[attribute1]:
                                closure.add(dependency)
                                
                #print("The closure of " + attribute + " is " + ', '.join(closure))
                return closure
                closure.clear()

    # find which X is the superkey of the relation
    for X in FDs:
        
        changed = True
        
        # add the attributes in X and Y to the closure set
        for attribute in X:
            closure.add(attribute)
 
            
        for Y in FDs[X]:
            closure.add(Y)
        
        # go through the Xs and see if we missed any
        while changed:
            changed = False
            for x in FDs:
                no_a = False
                for a in x:
                    if a not in closure:
                        no_a = True
                        break
                if not no_a:
                    for y in FDs[x]:
                        if y not in closure: # add the Ys that we missed                            
                            closure.add(y)
                            changed = True
        
        # if the user requested the closure of some attributes, print it
        if X in user_data:
            #print("The closure of " + X + " is " + ', '.join(closure))
            return closure
    
        # if the closure is all attributes in the relation, it is a superkey
        if closure.issuperset(R): 
            superkey = X
            return superkey
        
        closure.clear()
    
    # there is no superkey?    
    return 'oops'


def minimalCover(R, FDs):
    
    decomposedFDs = decomposeFDs(R, FDs)
    
    decomposedFDs = eliminateRedundantLHS(R, decomposedFDs, FDs)

    decomposedFDs = eliminateRedundantFDs(R, decomposedFDs, FDs)
    
    return decomposedFDs


def eliminateRedundantFDs(R, decomposedFDs, FDs):  
    
    redundancies = set()

    decomposedFDsdict = {}
    
    for FD in decomposedFDs:
        if FD[0] in decomposedFDsdict.keys():
            value = decomposedFDsdict[FD[0]]
            value = value + FD[1]  
            decomposedFDsdict.pop(FD[0])
            decomposedFDsdict[FD[0]] = value

        else:
            decomposedFDsdict[FD[0]] = FD[1]
    
    for FD in decomposedFDs:
        if len(decomposedFDsdict[FD[0]]) == 1:
            
            user_data = str(FD[0])
            user_data = [user_data]
            
            value = decomposedFDsdict[FD[0]]
            
            decomposedFDsdict.pop(FD[0])

            closure = findClosure3Nf(R, decomposedFDsdict, [str(FD[0])])

            if FD[1] in closure:
                redundancies.add(FD[1])
            
            decomposedFDsdict[FD[0]] = value

        else:
            for attribute in decomposedFDsdict[FD[0]]:
                originalValue = decomposedFDsdict[FD[0]]
                
                newValue = originalValue.replace(attribute, "")

                
                decomposedFDsdict.pop(FD[0])

                decomposedFDsdict[FD[0]] = newValue
                
                closure = findClosure3Nf(R, decomposedFDsdict, [str(FD[0])])
                            
                if attribute in closure:
                    redundancies.add((FD[0], attribute))  
                
                decomposedFDsdict.pop(FD[0])
   
                decomposedFDsdict[FD[0]] = originalValue                
                


    for redundantDependancy in redundancies:
        decomposedFDs.remove(redundantDependancy)
        
    return decomposedFDs


def eliminateRedundantLHS(R, decomposedFDs, FDs):
    
    checkLHS = []
    
    for FD in decomposedFDs:
        if len(FD[0]) > 1:
            checkLHS.append(FD)
            
    redundancies = []
    
    #comboLength = []
    
    for FD in checkLHS:
        comboLength = []
        attributes = list(FD[0])
        comboLength += itertools.combinations(attributes, len(FD[0])-1)
        
        #print(comboLength)
        
        for newCombo in comboLength:
            
            newLHS = "".join(newCombo)
            user_data = [newLHS]
            newClosure = findClosure3Nf(R, FDs, user_data)

            if FD[1] in newClosure:
                
                decomposedFDs.remove(FD)
                decomposedFDs.append((newLHS, FD[1]))
                
        
    return decomposedFDs


def decomposeFDs(R, FDs):
    
    decomposedFDs = []
        
    for FD in FDs: 
        if len(FDs[FD]) > 1:
              
            for index in range(len(FDs[FD])):
                #decomposedFDs.append(FD + "->" + FDs[FD][attribute])
                decomposedFDs.append((FD, FDs[FD][index]))
                    
        else: # RHS has only one attribute, cannot be further decomposed
                
            #decomposedFDs.append(FD + "->" + FDs[FD])
            
            decomposedFDs.append((FD, FDs[FD]))
            
    return decomposedFDs   

# ---------------------- TASK 3 ----------------------

def createTablesBCNF(conn, c, decomposition, listTables, types):
    
    out_table_list = []
    
    for schema in decomposition:
        nameR = 'Output_' + listTables[0][6:] + '_'
        nameFD = 'Output_FDs_' + nameR[7:]        
        R = schema[0]
        FD = schema[1]
        primary_key = schema[2]
        attribute = ''
        attributes_and_types = ''
        
        nameR += ''.join(R)
        nameFD += ''.join(R)
        
        out_table_list.append(nameR)
        out_table_list.append(nameFD)
        
        # pair up the attributes with their types
        for pair in types:           
            i = 0
            while pair[i] != ' ':
                attribute += pair[i]
                i += 1                
            if attribute in R:
                attributes_and_types += pair + ', '            
            attribute = ''
        
        # put the primary key in the correct format
        if primary_key == None:
            primary_key = ', '.join(R)
        else:
            primary_key = ', '.join(primary_key)
            
        # put the FDs in the correct format    
        FDs = ''
        no_FDs = False
        if FD == {}:
            no_FDs = True
        if not no_FDs:
            for key, value in FD.iteritems():
                FDs += "('" + key + "', '" + value + "'), "
            FDs = FDs[:-2]  
        
        drop = "DROP TABLE IF EXISTS " + nameR + ";"
        c.execute(drop)
            
        # create the relation table, to be populated with data later
        create_R = "CREATE TABLE " + nameR + " (" + attributes_and_types + "PRIMARY KEY (" + primary_key + "));"
        try:    
            c.execute(create_R)
        except sqlite3.OperationalError:
            print(" ")
            print("ERROR: table already exists.")
            conn.rollback()
            return 
        
        drop = "DROP TABLE IF EXISTS " + nameFD + ";"
        c.execute(drop)  
        
        # create the FDs table and add the FDs
        create_FD = "CREATE TABLE " + nameFD + " (LHS TEXT, RHS TEXT);"
        try:    
            c.execute(create_FD)   
        except sqlite3.OperationalError:
            print(" ")
            print("ERROR: table already exists.")
            conn.rollback()
            return        
        
        # insert functional dependencies into the table
        if not no_FDs:
            fill_FD = "INSERT INTO " + nameFD + " VALUES " + FDs + ";"
            c.execute(fill_FD)
    
        conn.commit()

    print(" ")
    print("*** Tables Created! ***")
    
    return out_table_list


def createTables(conn, c, decomposition, listTables, types, primaryKeys):
    
    print("********")
    print(decomposition)
    print(listTables)
    print(types)
    print(primaryKeys)
    print("********")
    nameR = 'Output_' + listTables[0][6]
    nameFD = 'Output_FDs_' + nameR[7]
    out_table_list = []
    index = 1
    
    for schema in decomposition:
        nameR = 'Output_' + listTables[0][6]
        nameFD = 'Output_FDs_' + nameR[7]        

        nameR += str(index)
        nameR += "_"
        
        nameFD += str(index)
        nameFD += "_"        

        R = schema[0]
        FD = schema[1]

        print(primaryKeys)
        primary_key = primaryKeys[index-1][0]


        attribute = ''
        attributes_and_types = ''
        
        nameR += str(schema[0][0])
        nameFD += str(schema[0][0])
        
        out_table_list.append(nameR)
        out_table_list.append(nameFD)
        
        # pair up the attributes with their types
        for pair in types:
            i = 0
            attribute = ""
            while pair[i] != ' ':
                attribute += pair[i]
                i += 1
                
            if attribute in R[0]:
                attributes_and_types += pair
                attributes_and_types += ", "
        
        attributes_and_types = attributes_and_types[:-1]
        attributes_and_types = attributes_and_types[:-1]

                
        FDs = []
        for item in FD:
            FDs.append((item[0], item[1]))
        
        createTableNameBE = " CREATE TABLE " + nameR + " (" + attributes_and_types + ", PRIMARY KEY ("
        
        if len(primary_key) == 1:
            createTableName = createTableNameBE + primary_key + ")); "
            
            #print(createTableName)

        else:
            numberPK = len(primary_key)
            index1 = 0
            while index1 < numberPK:
                createTableNameBE += primary_key[index1] + ", "
                index1 += 1

            
            createTableNameBE = createTableNameBE[:-1]
            createTableNameBE = createTableNameBE[:-1]
            
            createTableNameBE += "));"
            createTableName = createTableNameBE
            #print(createTableName)
        drop = "DROP TABLE IF EXISTS " + nameR + ";"
        c.execute(drop)    
        
        try:    
            c.execute(createTableName)
            
        except sqlite3.OperationalError:
                print(" ")
                print("ERROR: table already exists.")
                conn.rollback()
                return        
        
        conn.commit() 

        # create the FDs table and add the FDs
        createTableName = " CREATE TABLE " + nameFD + " (LHS TEXT, RHS TEXT); "
        
        drop = "DROP TABLE IF EXISTS " + nameFD + ";"
        c.execute(drop)        
        #print(createTableName)
        try:    
            c.execute(createTableName)
            
        except sqlite3.OperationalError:
            print(" ")
            print("ERROR: table already exists.")
            conn.rollback()
            return
        
        conn.commit()
        
        if FDs != []:
            if len(FDs) == 1:
                insertValue = str(FDs[0])
                insertValue = insertValue.replace("u", "")
                #print(insertValue)
                insertString = " INSERT INTO " + nameFD + " VALUES " + insertValue + ";"
                #print(insertString)
                c.execute(insertString)
                conn.commit()    
                
            else:
                for FD in FDs:
                    insertValue = str(FD)
                    insertValue = insertValue.replace("u", "")
                    #print(insertValue)                    
                    insertString = " INSERT INTO " + nameFD + " VALUES " + insertValue + ";"
                    #print(insertString)                   
                    c.execute(insertString)
                    conn.commit()
        
        index += 1
        
    print(" ")
    print("*** Tables Created! ***")    
    return out_table_list

# ---------------------- TASK 4 ----------------------

def fillData(c, conn, R, chosenOne, dataValues):
    
    c.execute(" SELECT * FROM sqlite_master WHERE type = 'table'; ")
    tables = c.fetchall()
    #print(tables)
    print("THIS IS R:")
    print(R)
    
    tableName = []
    for table in tables:
        tableName1 = table[4].split("(")
        string = str(tableName1[0][13:])
        string.replace("u", "")
        string.replace("'", "")        
        tableName.append(string)
    
    #print(tableName)
    
    chosenIndex = 0
    #print(tableName[chosenIndex])

    #print(chosenOne)
          
    while chosenOne != tableName[chosenIndex][:-1]:
        #print(tableName[chosenIndex])        
        chosenIndex += 1
        
    print(chosenIndex)
    col = tables[chosenIndex][4].split(",")
    col = col[:-1]
    colIndex = 0
    for c2 in col:
        string = str(c2)
        col.remove(c2)
        string.replace("u", "")
        string.replace("'", "")
        length = len(string)
        length = length - 4
        col.insert(colIndex, string[length - 1:length])
        colIndex += 1
        
    #print(col)
    
    relationalIndex = []
    
    for c1 in col:
        index = R.index(c1)
        relationalIndex.append(index)
        
    #print(relationalIndex)
    
    cleanData = []
    
    for dataString in dataValues:
        current = []
        for R1 in relationalIndex:
            current.append(dataString[R1])
            current1 = tuple(current)
        
        if current1 not in cleanData:
            cleanData.append(current1)
    
    for dataString1 in cleanData:
        insertV = str(dataString1)
        insertStr = " INSERT INTO " + chosenOne + " VALUES " + insertV + ";"
        #print(insertStr)
        c.execute(insertStr)
        conn.commit()
    
    #print(cleanData)
    return

# ---------------------- task 5 ----------------------

def findClosure(R, FDs, user_data):
    
    closure = set()
    closure_list = []
    superkey = ''

    # find which X is the superkey of the relation
    for X in FDs:
        
        changed = True
        
        # add the attributes in X and Y to the closure set
        for attribute in X:
            closure.add(attribute)
        for Y in FDs[X]:
            closure.add(Y)

        # go through the Xs and see if we missed any
        while changed:
            changed = False
            for x in FDs:
                no_a = False
                for a in x:
                    if a not in closure:
                        no_a = True
                        break
                if not no_a:
                    for y in FDs[x]:
                        if y not in closure: # add the Ys that we missed                            
                            closure.add(y)
                            changed = True

        
        # if the user requested the closure of some attributes, print it
        if X in user_data:
            #print("The closure of " + X + " is " + ', '.join(closure))
            for item in closure:
                closure_list.append(item)
            return superkey, closure_list
    
        # if the closure is all attributes in the relation, it is a superkey
        if closure.issuperset(R) and user_data == []: 
            superkey = X
            for item in closure:
                closure_list.append(item)
            return superkey, closure_list
        
        closure.clear()
    
    return superkey, closure_list

# ---------------------- task 6 ----------------------

def compareFDs(conn, c, F1names, F2names):
    
    R = []
    F1 = {}
    F2 = {}    
    # create a list of all of the attributes
    for table in F1names:
        statement = "SELECT * FROM " + table + ";"
        c.execute(statement)
        rows1 = c.fetchall()
        
        if rows1 == []:
            print(" ")
            print("There are no functional dependencies in a selected table.")
            return    
        
        for pair in rows1:
            F1[pair[0]] = pair[1]        
        
    for table in F2names:
        statement = "SELECT * FROM " + table + ";"
        c.execute(statement)
        rows2 = c.fetchall()
        
        if rows2 == []:
            print(" ")
            print("There are no functional dependencies in a selected table.")
            return          
        
        for pair in rows2:
            F2[pair[0]] = pair[1]
    
    for k in F1:
        for a in k:
            if a not in R:
                R.append(a)
        for y in F1[k]:
            if y not in R:
                R.append(y)
    for k in F2:
        for a in k:
            if a not in R:
                R.append(a)
        for y in F2[k]:
            if y not in R:
                R.append(y)    
    
    # for each key in F1, check to see if there is equivalent closure from F2  
    for X1 in F1:
        equivalent = False
        c1 = []
        for x1 in X1:
            c1.append(x1)
        for y1 in F1[X1]:
            c1.append(y1) 
            
        if X1 not in F2:            
            # see if the original key and its dependencies can be derived
            for X2 in F2:
                superkey, c2 = findClosure(R, F2, [X2])
                if set(c2).issuperset(set(c1)):
                    equivalent = True
                    break
                
        elif F1[X1] != F2[X1]:
            for X2 in F2:
                superkey, c2 = findClosure(R, F2, [X2])
                if set(c2).issuperset(set(c1)):
                    equivalent = True
                    break
        else:
            equivalent = True
        
        if not equivalent:
            print(" ")
            print("*** F1 and F2 are not equivalent ***")
            return
    
    if equivalent:
        print(" ")
        print("*** F1 and F2 are equivalent ***")
    else:
        print(" ")
        print("*** F1 and F2 are not equivalent ***")
    
    return

# ---------------------- task 7 ----------------------

def dropTables(out_table_list, conn, c):
    
    script = ""
    #print(out_table_list)
    for name in out_table_list:
        script += "DROP TABLE " + name + "; "
    
    #print(script)
    c.executescript(script)
    conn.commit()
    print(" ")
    print("*** Tables Dropped! ***")    

    return

# --------------------------------------------

def main():
    
    normalized_table = []
    task = ''
    superkey = ''
    n_type = ''
    out_table_list = []
    created = False
    #inputDatabase = raw_input("What is the database you would like to use? ")
    #conn = sqlite3.connect(inputDatabase)
    conn = sqlite3.connect('MiniProject2-InputExample-2.db')
    c = conn.cursor()
    #c.execute('PRAGMA foreign_keys=ON;')
    
    types, listTables, R, dataValues, FDs = readDatabase(conn, c)
    
    #normalized_table = [[['A', 'D', 'E'], {'A' : 'DE'}, ['A']], [['F', 'A', 'H'], {'F' : 'AH'}, ['F']], [['B', 'C', 'F', 'G'], {}, ['BCFG']]]
    #R = ['A','B','C','D','E','F','G','H']
    #FDs = {'ABH': 'C', 'A' : 'DE', 'BGH' : 'F', 'F' : 'ADH', 'BH' : 'GE'}    
    
    while task != '0':
        
        task = getTask()
        
        if task == '1':
            
            relationalIndex = 0
            for table in listTables:
                if len(table) == 2:
                    print("This is Relational Schema " + str(relationalIndex))
                    print(table[0])
                    print(table[1])
                
                relationalIndex += 1
            
            chosenSchema = raw_input("Please enter number of schema you would like to decompose ")
            
                
            Rel = R[int(chosenSchema)]
            Fds = FDs[int(chosenSchema)].copy()   
            
            Rel = list(Rel)
            
            print(Rel)
            print(Fds)
            
            normalized_table, primaryKeys = thirdNF(Rel, Fds, superkey)
            print(normalized_table)
            n_type = '3NF'

        elif task == '2':
            
            relationalIndex = 0
            for table in listTables:
                if len(table) == 2:
                    print("This is Relational Schema " + str(relationalIndex))
                    print(table[0])
                    print(table[1])
                
                relationalIndex += 1
            
            chosenSchema = raw_input("Please enter number of schema you would like to decompose ")
            
                
            Rel = R[int(chosenSchema)][:]
            Fds = FDs[int(chosenSchema)].copy()   
            
            print(Rel)
            print(Fds)
            
            #Rel = R[:]
            #Rel = list(Rel)
            #Fds = FDs.copy()
            normalized_table = BCNF(Rel, Fds)
            print(normalized_table)
            n_type = 'BCNF'

        elif task == '3':
            if normalized_table == []:
                print(" ")
                print("You haven't normalized a table yet.")
            else:
                if n_type == "3NF":
                    out_table_list = createTables(conn, c, normalized_table, listTables, types, primaryKeys)
                else:
                    out_table_list = createTablesBCNF(conn, c, normalized_table, listTables, types)

        elif task == '4':
            #if normalized_table != []:
             #   addData(out_table_list)
            if out_table_list != []:
                print(" ")
                print("This is a list of the Output Relation Tables")
                for table in out_table_list:
                    if "FDs" not in table:
                        print(table)
                
                chosen = False
                while not chosen:
                    chosenOne = raw_input("Please input name of table you have chosen: ")
                    
                    if chosenOne not in out_table_list:
                        print("That table does not exist! Please choose again.")
                    else:
                        chosen = True
                        
                print("This is the table you have chosen: " + chosenOne)
                
                print(dataValues)
                
                fillData(c, conn, R, chosenOne, dataValues)      
            else:
                print(" ")
                print("There are no Output Tables currently! Please do Task 3 first!")        
            
        elif task == '5':
            user_attributes = raw_input("Enter the attributes: ")
            user_FDs = raw_input("Enter the name of the table containing the FDs: ")
            superkey = findClosure3Nf(R, user_FDs, user_attributes)
            
        elif task == '6':
            if out_table_list != []:
                print("This is a list of the Output Funtional Dependency Tables")
                for table in out_table_list:
                    if "FDs" in table:
                        print(table)
                for table in listTables:
                    if "FDs" in table:
                        print(table)
                
                F1 = []
                F2 = []
                finished = False
                while not finished:
                    f = raw_input("Enter a table from the list to be included in F1 (0 when done): ")
                    if f == '0':
                        finished = True
                    else:
                        F1.append(f)
                finished = False
                while not finished:
                    f = raw_input("Enter a table from the list to be included in F2 (0 when done): ")
                    if f == '0':
                        finished = True
                    else:
                        F2.append(f)                                     
                compareFDs(conn, c, F1, F2)
                
            else:
                print(" ")
                print("There are no Output Tables currently! Please do Task 3 first!") 
            
        elif task == '7':
            dropTables(out_table_list, conn, c)
            out_table_list = []
    
    #print(FDs)
    #superkey, closure = findClosure3Nf(R, FDs, ['BH'])
    #print(superkey)
    
    print "Goodbye!"
    
    conn.close()
    

main()