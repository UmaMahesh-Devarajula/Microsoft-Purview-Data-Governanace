import sys
from Collections.collections import collections
from datasources.dataSources import dataSources

def datamap():    
        
    while True:
        print("""
        1. Collections
        2. Data Sources
        3. Go to Purview
        4. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
            collections()
        elif choice == "2":
            datasources()
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
