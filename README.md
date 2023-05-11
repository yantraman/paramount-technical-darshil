**Sr. Data Engineer (36230) – Technical Assessment**

**Introduction:**

Please complete the following technical assignment to the best of your abilities. The assignment is designed to assess your ability to work across a range of data types and combine them into a single queryable entity.

**Instructions:**

There are three simulated data structures generated from a collection of simulated data meant to emulate the data structure one might find for data from an online discussion community. Take the following three data sources, and combine into a single database structure, with one, or many related tables.

You will need to write the code to load each data source to the database you build, and run any preprocessing where needed. It is up to you what architecture/platform you use for the database, but be prepared to explain your choices - e.g. if MySQL used, why was this chosen over similar technologies like postgres or much different technologies like mongo dB. This is intentionally an open-ended task meant to assess ability to work through similar type open ended requests.

You will next run a series of queries (provided below) against the database you've constructed, and report back the results.

Finally, please provide answers to the discussion questions.

Please see 'deliverables' section at end of document for instructions on what we'd like to receive from you.

As a guideline, we are asking that all applicants to please not spend more than 6 hours on this task.

**Source Datasets:**

Data available here: [https://drive.google.com/drive/folders/1xVKkQ7aTuUF2-S3-Uyb2msOJUwFsm9TW?usp=share\_link](https://drive.google.com/drive/folders/1xVKkQ7aTuUF2-S3-Uyb2msOJUwFsm9TW?usp=share_link)

You will be working with data simulated in the style of data collected from an online discussion community. The structure is as follows:

The data consists of comments associated with posts. Each comment is associated with a post, and each post and comment is associated with a page. In this dataset the central entity is a comment.

The data consists of top-level comments, and first level replies. Top level comments are comments made on a post. First level replies are direct replies made to top level comments. Second level and higher replies (replies to replies of top comments) are not included.

Second level comments are associated with all of the following: top level comments, post

ID, page ID. Top level comments are associated with all of the following: post ID, page ID. The data hierarchy is as follows:

Throughout the dataset ID's are coded/labeled as 'h\_id'.

![](RackMultipart20230510-1-fn9sgc_html_cac660d686830453.png)

Each page ID can be associated with 1 or more post IDs. Each post ID can be

associated with 1 or more top level comment IDs. Each top level comment ID can be associated

with 0 or more reply level comments.

**Dataset 1: Comment\_text.zip**

Description: Folder containing compressed csv files Schema:
 h\_id: (string) Comment ID

message: (string) Comment text. The comment text is generated, and so expect the text

to look strange, such as : ​hx! mgx.. Cm Bch! x hx p xxwy! ... 😂😂😂😂

Notes:
 The comment IDs in this dataset do not match 1:1 with comment IDs present in other datasets. For queries, report only where IDs match across datasets.

**Dataset 2: Comment\_info\_jsonl.zip**

Description: Folder containing JSONL formatted records. Records contain information around

the comments.

![](RackMultipart20230510-1-fn9sgc_html_449020881ee390fa.png)

Schema:

![](RackMultipart20230510-1-fn9sgc_html_9f5068173f8cb135.jpg)

At any given level, an ID (coded as h\_id in the dataset) represents the ID for the object at that level. For example, 'h\_id' at Page information is page ID, and 'h\_id' at post information is post ID.

Notes:
 Due to the nature of how the data is originally collected, any given comment may appear in multiple records within or across JSONL files. Furthermore, in some cases, the like\_count and comment\_count values may be augmented across duplicate IDs. In these situations, use the record where values are highest for "created\_time","like\_count","comment\_count", in that order.
 The IDs in this dataset do not match 1:1 with IDs present in other datasets. For queries, report only where IDs match across datasets.

**Dataset 3: Post\_meta.zip**

Description: Folder containing compressed parquet files. Structured data containing post level information.
 Schema:

post\_h\_id: ​(string)​: Post ID.
 post\_created\_time: ​(timestamp): Created time of post.
 name\_h\_id: ​(string): Generated value, representing name of page post was made on. type: ​(string): Code representing the type of post. Four possible values, l/s/v/p

Notes:
 The post IDs in this dataset do not match 1:1 with post IDs present in other datasets. For queries, report only where IDs match across datasets.

**Queries to run**

Once data is organized and sits in a database, please run the following queries and return the results.
 In all queries please report back only on records where IDs match at least across the number of datasets specified

Post ID: Must match across at least two of the datasets. Comment ID: Must match across all three datasets.

1. 1)  Which day had the highest number of top level comments (excluding replies)?
2. 2)  Report back the number of comments, and comment up\_likes, per type, for all

comments (top level and replies) made ​_after_​ 2018-01-10

1. 3)  For each page, find the average length per comment (number of characters). Include top

level comments and replies. What are the top 5 pages, sorted by highest average length of comment. Please provide page names, page ids, and values for average length of comment.

1. 4)  Where data exists for ​_both top comment, and corresponding replies_​, what are the top 5 top level comments with most replies. Report back page name, post ID, comment ID, number replies to top level comment & text of top level comment.

**Discussion questions**

Please keep answers between 100 - 250 words per question below. Assume you are writing for a technical audience. Feel free to include hyperlinks to any external material as needed.

1. How you would approach this problem if each dataset was 100 GB instead of less than 100 MB per dataset like in the assignment.
 For each dataset type, how would you handle processing at this scale. How would your implementations change from this assignment? If you would choose different pipelines or tools, please discuss why you made those choices.
2. What about if you expected 10 GB of new data, for each source, daily, for the next year? For each dataset type, how would you handle processing at this scale. How would your implementations change this assignment? If you would choose different pipelines or tools than (1), please discuss why you made those choices.
3. How would you go about deploying this solution to a production environment? Would you make any changes to the architecture outline above? Please discuss any anticipated methods for automating deployment, monitoring ongoing processes, ensuring the deployed solution is robust (as little downtime as possible), and technologies used.

**Deliverables**

1. Please provide all code used to implement your solution.
 Share via github or zip file. We will read your code, and may try to run parts of it, so please be organized.
 In verifying your queries, if any of your results differ to ours, we will refer to the submitted code to try and understand why the difference occurred. So do your best to include ​_all_ steps taken in processing the data and building the database.
 Please report any environmental dependencies we'd need to get the code to run, or if applicable a copy of the container(s) used, or container build script(s).
2. Please provide answers to the queries, as numbered.
3. Please provide answers to the to discussion questions, as numbered.