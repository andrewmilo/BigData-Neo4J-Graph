import sys
import thread
import time
import CollaboratorManager

def main():
    collabManager = CollaboratorManager.CollaboratorManager(sys.argv) # Create instance of the colloborator manager
    print "****Loading the database****"
    collabManager.DbLoader() # Load the database
    thread.start_new_thread(collabManager.trustedColleagues,()) #Start a thread to create the trusted colleagues relationship
    
    print "Welcome to the Collaborator DB Manager"
    go = True
    while go:
        query= raw_input("Press 1 to run the Query Collaborator. Press 2 to run Query Colleagues of Colleagues. Press anything else to exit. ")
        if query == '1':
            print "Query Collaborator"
            collabManager.queryCollaborator() #Run the queryCollaborator
        elif query == '2':
            print "Query Col of Col"
            collabManager.queryColOfCol() #Run the query Colleagues of Colleagues
        else:
            print "Thank you for using the Collaborator Manager"
            go = False
          
if __name__ == '__main__':
    start = time.time() 
    main()
    end = time.time() - start
    print "Time to complete:", end
