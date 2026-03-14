import sys
from Collections.collections import collections
from datasources.dataSources import dataSources
from Metadata.exportMetadata import exportmetadata

def datamap():    
        
    while True:
        print("""
        1. Collections
        2. Data Sources
        3. Export Metadata
        4. Go to Purview
        5. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
            collections()
        elif choice == "2":
            dataSources()
        elif choice == "3":
            exportmetadata()
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
