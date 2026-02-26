import sys
from datasources.registerdatasource import register_datasource
from Collections.listCollections import listCollections

def datamap():    
        
    while True:
        print("1. Register a data source \n 2. scan a data source \n 3. go to previous menu \n 4. exit")
        choice = input("Enter your choice: ")
        if choice == "1":
            register_datasource()
        elif choice == "2":
            listCollections()
        elif choice == "3":
                import purview# Import inside the function
                purview.purview()
        elif choice == "4":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    datamap()
