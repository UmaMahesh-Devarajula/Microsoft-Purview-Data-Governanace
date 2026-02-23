import sys
import purview
from datasources.registerdatasource import register_datasource

def datamap():    
    print("1. Register a data source \n 2. scan a data source \n 3. go to previous menu \n 4. exit")
    
    while True:
        choice = input("Enter your choice: ")
        if choice == "1":
            register_datasource()
        elif choice == "2":
            create_scan()
        elif choice == "3":
            purview()
        elif choice == "4":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
