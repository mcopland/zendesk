#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


# Create dataframe from CSV file
df = pd.read_csv('sheets/organizations.csv', na_filter=False)


# In[3]:


# Rename columns to map to Zendesk fields
df = df.rename(columns={'id': 'external_id'})


# In[4]:


# Ensure required columns are unique
print('{:<30}{}'.format("\'external_id\' is unique:", df['external_id'].is_unique))
print('{:<30}{}'.format("\'name\' is unique:", df['name'].is_unique))


# In[5]:


# Assign values to new 'organization_fields' column
new_col = [{'merchant_id': df['merchant_id'][i]} for i in range(len(df))]
# Add new column to dataframe
df['organization_fields'] = new_col


# In[6]:


# Remove unnecessary 'merchant_id' column
del df['merchant_id']


# In[7]:


# Convert 'external_id' column to string to match required input
df['external_id'] = df['external_id'].apply(str)


# In[8]:


# Transform 'domain_names' and 'tags' into a list of strings
def str_to_list(row, col) -> list:
    if isinstance(row[col], str):
        if len(row[col]) < 3:
            return ['mcopland'] if col == 'tags' else []
        else:
            # Split string into list of strings
            new_list = row[col][1:-1].split(', ')
            # Remove unnecessary characters
            for i, entry in enumerate(new_list):
                if entry[1] == '(' and entry[-2] == ')':
                    # Strip single quotes and parentheses
                    new_list[i] = entry[2:-2]
                else:
                    # Strip single quotes only
                    new_list[i] = entry[1:-1]
            if col == 'tags':
                # Tag all organizations with my name so that they can be easily located later
                new_list.append('mcopland')
            return new_list
    # Default return
    return row[col]
            
df['domain_names'] = df.apply(lambda row: str_to_list(row, 'domain_names'), axis=1)
df['tags'] = df.apply(lambda row: str_to_list(row, 'tags'), axis=1)


# In[9]:


# View dataframe information
df.info()
df.head()


# In[10]:


# Split df into chunks of 100 for batch import
n = 100
chunks = [df[i:i+n] for i in range(0, df.shape[0], n)]
print(len(chunks))


# In[11]:


# Export to JSON file
for i in range(len(chunks)):
    filename = f'./organizations_{i}.json'
    # records : list [{column -> value}, ... , {column -> value}]
    chunks[i].to_json(filename, orient='records')

