import os
from py2neo import Graph
from py2neo.packages.httpstream import http
http.socket_timeout = 9999

class CollaboratorManager(object):
    #csvLoader will be needed each time we load a file. It will skip the header and commit every 10000 rows
    __csvLoader = """USING PERIODIC COMMIT 10000
        LOAD CSV FROM
        'file:///{DIR}' AS line WITH line SKIP 1
        {QUERY}"""
    
    #Constructor which shall set the folder path from the first program argument and the graph binding from a configuration file in the second program file
    def __init__(self, argv):
        self.__folderPath = argv[1]
        self.__userInfo = open(argv[2], 'r').readlines()
        link = "http://{a}:{b}@localhost:7474/db/data/".format(a = self.__userInfo[0].strip(' \n'), b = self.__userInfo[1])
        self.__graph = Graph(link)
        #Array of fileNames sorted backwards. Recieved from the first program argument
        self.__fileNames = sorted(os.listdir(argv[1]), reverse = True)
    
    #Checks if the database contains any users
    def checkDB(self):
        __rec = self.__graph.cypher.execute("MATCH(user:User) return user limit 1")
        if len(__rec) > 0:
            return False
        return True

        self.__graph.cypher.execute()

    def DbLoader(self):
        #Create an index on the user id and organization name for faster lookup       
        self.__graph.cypher.execute("CREATE INDEX ON :User(id)")
        self.__graph.cypher.execute("CREATE INDEX ON :Organization(organization_name)")
        #go through each file
        for filename in self.__fileNames: 
            if filename == "user.csv":
                #If there are users in the database, use merge to avoid duplicates. Otherwise use Create for speedup
                #User will have properties : id, first_name, last_name
                if self.checkDB() == True:
                    query = "CREATE (:User {id: line[0], first_name: line[1], last_name: line[2]})"
                else: query = "MERGE (:User {id: line[0], first_name: line[1], last_name: line[2]})"
                
            elif filename == "interest.csv":
                #Find the user specified in the row of the file, create the interest, and the relationship between the user and that interest
                query = """  
                   MATCH (user:User {id : line[0]}) 
                   MERGE (interest:Interest {interest_name: line[1]}) 
                   MERGE (user) -[:HAS_INTEREST {interest_level: line[2]}]-> (interest)
                   """               
                             
            elif filename == "skill.csv":
                #Find the user specified in the row of the file, create the skill, and the relationship between the user and that skill
                query = """  
                   MATCH (user:User {id: line[0]}) 
                   MERGE (skill:Skill {skill_name: line[1]})  
                   MERGE (user) -[:HAS_SKILL {skill_level: line[2]}]-> (skill)                 
                """
                
            elif filename == "project.csv":
                #Find the user specified in the row of the file, create the project, and the relationship between the user and that project
                query = """
                    MATCH (user:User {id: line[0]}) 
                    MERGE (project:Project {project_title: line[1]})  
                    MERGE (user) -[:WORKS_ON]-> (project)  
                """
                
            elif filename == "organization.csv":
                #Find the user specified in the row of the file, create the organization, and the relationship between the user and that organization
                query = """  
                   MATCH (user:User {id :line[0]}) 
                   MERGE (org:Organization {organization_name: line[1], organization_type: line[2]})  
                   MERGE (user) -[:WORKS_FOR]-> (org)                 
                """
                   
            elif filename == "distance.csv":
                #Find the organizations specified in the row of the file and create the distance relationship between the two organizations
                query = """  
                   Match (org1:Organization {organization_name: line[0]}),(org2:Organization {organization_name: line[1]}) 
                   MERGE (org1) -[:DISTANCE {distance: line[2]}]-> (org2)           
                """
            #Execute the proper query depending on the filename as stated in the csvLoader string
            self.__graph.cypher.execute(self.__csvLoader.format(DIR = self.__folderPath + "/" + filename, QUERY = query))
        
    def queryCollaborator(self):
        #Given an organization type, user id, and maximum distance, display other users who share the same interests or skills 
        #and working in the same or different organization within the given distance from the given users organization.
        organizationType = raw_input("Please enter the type of organization: ")
        userID = raw_input("Please enter a %s user's id: " %organizationType) 
        distance = raw_input("Please enter the maximum distance: ")
        #Output is ordered d by total interest weight + total skill weight. Total interest weight is found by summing both users interest levels.
        #Total skill weight is found by taking the max of the 2 users skill levels. This is seen in the CASE clause in the query
        #We also check that the two users do in fact share at least 1 interest or skill as seen in the WHERE is Not null OR s IS NOT null
        records= self.__graph.cypher.execute(
            """MATCH (user:User {id: \""""+ userID+ "\"""""}) -[:WORKS_FOR]-> (org1:Organization {organization_type : \""""+ organizationType+ "\"""""}),
            (user2:User) -[:WORKS_FOR]->(org2:Organization)
            OPTIONAL MATCH (org1)-[d:DISTANCE]-(org2)
            WITH user, user2, org1, org2, d
            WHERE org1.organization_name = org2.organization_name OR toFloat(d.distance) <= """ + distance + """
            WITH user, user2, org2
            OPTIONAL Match (user) -[IW1:HAS_INTEREST] ->(i:Interest)<- [IW2:HAS_INTEREST]- (user2)
            Optional MATCH (user) -[SW1:HAS_SKILL] ->(s:Skill)<- [SW2:HAS_SKILL]- (user2)
            WITH user2, org2, i, s, IW1, IW2, SW1, SW2, CASE WHEN (toInt(SW1.skill_level) < toInt(SW2.skill_level)) THEN SW2 ELSE SW1 END as SW 
            WHERE i IS NOT null OR s IS NOT null
            RETURN DISTINCT user2.first_name as First_Name, user2.last_name as Last_Name, org2.organization_name as Organization, str(collect(distinct i.interest_name)) as Shared_Interests, 
            str(collect(DISTINCT s.skill_name)) as Shared_Skills, sum(DISTINCT toInt(IW2.interest_level)) + sum(DISTINCT toInt(IW1.interest_level)) + sum(DISTINCT toInt(SW.skill_level)) as Total_Weight
            ORDER BY Total_Weight DESC"""
        )
        #If there is a record, print it. Otherwise alert the user that no matches were found
        if len(records) > 0:
            print records
        else: print "No matches were found"
    
    def trustedColleagues(self):
        #Creates a trusted colleagues relationship which is defined by two users who work on the same project
        query = """MATCH (user1:User) -[:WORKS_ON]-> (:Project) <- [:WORKS_ON] - (user2:User) 
                   MERGE (user1) -[:TRUSTED_COLLEAGUE]-(user2) """
        self.__graph.cypher.execute(query)
        
    def queryColOfCol(self):
        #Given a user id and optional interests. The query finds all colleagues of colleagues who have those interests if specified
        userID = raw_input("Please enter the user's id: ")
        interests = raw_input ("Optionally, please enter user %s's colleagues of colleagues interests seperated by a space: " % userID).split()

        records= self.__graph.cypher.execute("MATCH (user:User {id: \"" + userID + "\"})-[:TRUSTED_COLLEAGUE*2]-(colOfCol:User)," +
                                          """(colOfCol)-[:HAS_INTEREST]->(i:Interest)
                                             WHERE user.id <> colOfCol.id AND (i.interest_name in {Interests} OR length({Interests}) = 0) AND NOT((user)-[:TRUSTED_COLLEAGUE]-(colOfCol))
                                             RETURN DISTINCT colOfCol.first_name as First_Name ,colOfCol.last_name as Last_Name, str(collect(i.interest_name)) as Has_Interests """.format(Interests = interests))
        
        #If there is a record, print it. Otherwise alert the user that no matches were found
        if len(records) > 0:
            print records
        else: print "No matches were found"