from elasticsearch import Elasticsearch

# Initialize the Elasticsearch client
es = Elasticsearch(['http://localhost:9200'])

def delete_all_indices():
    # Get all indices
    indices = es.indices.get_alias().keys()
    
    # Print the indices that will be deleted
    print("The following indices will be deleted:")
    for index in indices:
        if not index.startswith("."):
            print(index)
    
    # Confirm with the user
    confirm = input("Are you sure you want to delete all indices? This action cannot be undone. (yes/no): ")
    
    if confirm.lower() == 'yes':
        # Delete all indices
        for index in indices:
            if not index.startswith("."):
                es.indices.delete(index=index, ignore=[400, 404])
        print("All indices have been deleted.")
    else:
        print("Operation cancelled. No indices were deleted.")

# Run the function
if __name__ == "__main__":
    delete_all_indices()