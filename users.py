#!/usr/bin/env python
# coding: utf-8

# In[1]:


import glob
import json
import numpy as np
import os
import pandas as pd


# In[2]:


# Create dataframe from CSV file
df = pd.read_csv('sheets/users.csv', na_filter=False)


# In[3]:


# Rename columns to map to Zendesk fields
df = df.rename(columns={'id': 'external_id'})


# In[4]:


# Verify 'name' count as it is a required value
print('{:<40}{}'.format('Each \'name\' cell contains a value:', (df['name'].values == '').sum() == 0))
# Ensure required columns are unique
print('{:<40}{}'.format("\'external_id\' is unique:", df['external_id'].is_unique))


# In[5]:


# Assign values to new 'user_fields' column
new_col = [{
    'group': df['group'][i],
    'api_subscription': df['api_subscription'][i],
    'employee_id': df['employee_id'][i],
    'promotion_code': df['promotion_code'][i]
} for i in range(len(df))]
# Add new column to dataframe
df['user_fields'] = new_col


# In[6]:


# Remove unnecessary columns
del df['active']
del df['group']
del df['api_subscription']
del df['employee_id']
del df['promotion_code']


# In[7]:


# Convert 'external_id' column to string to match required input
df['external_id'] = df['external_id'].apply(str)


# In[8]:


# Convert empty 'email' strings to NaN values for easy processing
df['email'].replace(r'^\s*$', np.nan, inplace=True, regex=True)
# Gather all duplicate emails except first in series
duplicate_emails = df[df.duplicated(['email'], keep='first')]
# Drop NaN values
duplicate_emails = duplicate_emails[duplicate_emails['email'].notna()]

display(duplicate_emails)


# In[9]:


# Count number of matching files from API output
path = os.getcwd()
json_file_count = len(glob.glob1(os.getcwd(), 'org_response_pg*.json'))
print(json_file_count)


# In[10]:


# Gather generated 'id' values from created Organizations
frames = []
for i in range(json_file_count):
    filename = f'org_response_pg{i}.json'
    with open(filename, 'r') as f:
        new_records = json.loads(f.read())
    # Flatten data
    new_df = pd.json_normalize(new_records, record_path =['organizations'])
    frames.append(new_df)

# Concatenate multiple dataframes
org_id_map = pd.concat(frames)
# Drop unnecessary information and rename columns
org_id_map.drop(org_id_map.columns.difference(['id', 'external_id']), 1, inplace=True)
org_id_map = org_id_map.rename(columns={'id': 'zendesk_id', 'external_id': 'organization_id'})

# Transform into dictionary for easy replacement
id_dict = dict(zip(org_id_map['organization_id'], org_id_map['zendesk_id']))


# In[11]:


def organization_to_list(org_id):
    if isinstance(org_id, str):
        # No organizations assigned
        if len(org_id) < 3:
            return []
        # Multiple organizations found
        if org_id[0] == '[' and org_id[-1] == ']':
            # Split string into list of strings
            new_list = org_id[1:-1].split(', ')
            # Remove unnecessary single quotes
            for i, entry in enumerate(new_list):
                # Map proper 'organization_id' values to users (values taken after Organization creation)
                new_list[i] = id_dict[entry[1:-1]] if entry[1:-1] in id_dict else entry[1:-1]
            return new_list
        # Map proper 'organization_id' values to users (values taken after Organization creation)
        return id_dict[org_id] if org_id in id_dict else org_id
    # Default return
    return org_id

# Transform 'organization_id' into a list of strings
df['organization_id'] = df.apply(lambda row: organization_to_list(row['organization_id']), axis=1)


# In[12]:


# Gather user id values that contain duplicate emails
duplicate_ids = set(duplicate_emails['external_id'])

# Transform 'tags' into a list of strings
def tags_to_list(row, col):
    if isinstance(row[col], str):
        if len(row[col]) < 3:
            new_list = []
        else:
            # Split string into list of strings
            new_list = row[col][1:-1].split(', ')
        # Remove unnecessary single quotes
        for i, entry in enumerate(new_list):
            new_list[i] = entry[1:-1]
        # Handle duplicate emails
        if row['external_id'] in duplicate_ids:
            # Add tags for 'duplicate_email' and 'email' value
            new_list.append('duplicate_email')
            new_list.append(row['email'])
        # Tag all users with my name so that they can be easily located later
        new_list.append('mcopland')
        return new_list
    # Default return
    return row[col]
    
# Delete duplicate values in 'email' field
def handle_email_field(row, col):
    return '' if row['external_id'] in duplicate_ids else row[col]

# Convert NaN values back to empty strings
df['email'].replace(np.nan, '', inplace=True)

# Apply specified functions along set columns of dataframes
df['tags'] = df.apply(lambda row: tags_to_list(row, 'tags'), axis=1)
df['email'] = df.apply(lambda row: handle_email_field(row, 'email'), axis=1)


# In[13]:


# Ensure 'email' column is now unique
total = len(df)
blanks = (df['email'].values == '').sum()
# Subtract the one blank value that is counted in unique
unique = len(df['email'].unique()) - 1
print(total - blanks - unique == 0)


# In[14]:


organization_memberships = []

def set_primary_organization(row, col):
    if isinstance(row[col], list):
        if len(row[col]) >= 1:
            # 'organization_id' must be an int value
            int_list = list(map(int, row[col]))
            if len(row[col]) > 1:
                # Store further values in dictionary
                for org in int_list[1:]:
                    organization_memberships.append({'user_id': int(row['external_id']), 'organization_id': org})
            # Set first index as primary organization
            return int_list[0]
    # Default return
    return row[col]

# Set first 'organization_id' as primary and store other values for later assignment
df['organization_id'] = df.apply(lambda row: set_primary_organization(row, 'organization_id'), axis=1)


# In[15]:


# Add 'verified' column to create users without sending a verification email
df['verified'] = True


# In[16]:


# View dataframe information
df.info()
df.head()


# In[17]:


# Split df into chunks of 100 for batch import
n = 100
chunks = [df[i:i+n] for i in range(0, df.shape[0], n)]
# Split organization_memberships into chunks of 100 for batch import
org_chunks = [organization_memberships[i:i+n] for i in range(0, len(organization_memberships), n)]


# In[18]:


# Export to JSON files
for i in range(len(chunks)):
    filename = f'./organization_assignments_{i}.json'
    with open(filename, 'w') as outfile: 
        json.dump(organization_memberships, outfile)

for i in range(len(chunks)):
    filename = f'./users_{i}.json'
    # records : list [{column -> value}, ... , {column -> value}]
    chunks[i].to_json(filename, orient='records')

