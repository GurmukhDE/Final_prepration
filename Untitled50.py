#!/usr/bin/env python
# coding: utf-8

# In[1]:


list1 = [1,2,3,4]
list2 = list1


# In[2]:


list1


# In[3]:


list2


# In[4]:


list1, list2


# In[6]:


list1[1] = 1000


# In[7]:


list2


# In[ ]:


list1


# In[15]:


a = [1,2,3,4]


# In[16]:


a


# In[10]:


#a.append([4,5])


# In[17]:


a


# In[18]:


a.extend([4,5])


# In[19]:


a


# In[21]:


lis = [1,2,34,45,5]

sqr_list = list(map(lambda x: x*2, lis))

print(sqr_list)


# In[22]:


lis = [1,2,34,45,5]

sqr_list = list(filter(lambda x: x*2, lis))

print(sqr_list)


# In[25]:


from functools import reduce


# In[28]:


num = [1,2,3,5,5,6]
result = reduce(lambda x,y: x/y,num)
print(result)


# In[ ]:




