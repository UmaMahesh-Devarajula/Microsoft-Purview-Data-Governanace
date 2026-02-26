import sys
from datasources.registerdatasource import register_datasource
from Collections.listCollections import listCollections
from Collections.restoreCollections import recreate_from_csv

def datamap():    
        
    while True:
        print("1. Register a data source \n 2. export collections \n 3. restore collections using bkp file 4. go to previous menu \n 5. exit")
        choice = input("Enter your choice: ")
        if choice == "1":
            register_datasource()
        elif choice == "2":
            listCollections()
        elif choice == "2":
            recreate_from_csv()
        elif choice == "4":
                import purview# Import inside the function
                purview.purview()
        elif choice == "5":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    datamap()
