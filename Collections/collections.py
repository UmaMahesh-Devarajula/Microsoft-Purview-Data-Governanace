import sys
from Collections.createCollection import createCollection
from Collections.listCollections import listCollections
from Collections.deleteCollection import deleteCollection
from Collections.restoreCollections import restoreCollections
from Collections.exportCollections import exportCollections

def collections():
    while True:
        print("""
        1. Create or Update Collection
        2. List Collections
        3. Delete Collection
        4. Export Collections
        5. Restore Collections (using the exported file)
        6. Go to Data Map
        7. Go to Purview 
        8. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
            createCollection()
        elif choice == "2":
            listCollections()
        elif choice == "3":
            deleteCollection()
        elif choice == "4":
            exportCollections()
        elif choice == "5":
            restoreCollections()
        elif choice == "6":
            from DataMap.dataMap import datamap
            datamap()
        elif choice == "7":
                import purview# Import inside the function
                purview.purview()
        elif choice == "8":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    collections()