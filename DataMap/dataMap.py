import sys
from datasources.registerdatasource import register_datasource
from Collections.restoreCollections import restoreCollections
from Collections.exportCollections import exportCollections
from datasources.exportdatasources import exportdatasources
from datasources.restoredatasources import restoredatasources
from Metadata.exportMetadata import exportMetadata
from Metadata.restoreMetadata import restoreMetadata

def datamap():    
        
    while True:
        print("""
        1. Export default domain's collection hierarchy
        2. Restore default domain's collection hierarchy
        3. Export data sources
        4. Restore data sources
        5. Register a data source
        6. Export Metadata
        7. Import Metadata
        8. Go to Main menu
        9. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
            exportCollections()
        elif choice == "2":
            restoreCollections()
        elif choice == "3":
            exportdatasources()
        elif choice == "4":
            restoredatasources()
        elif choice == "5":
            register_datasource()
        elif choice == "6":
            exportMetadata()
        elif choice == "7":
            restoreMetadata()
        elif choice == "8":
                import purview# Import inside the function
                purview.purview()
        elif choice == "9":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    datamap()
