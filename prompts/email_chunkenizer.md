
**Now, your goal is to split the document given before into chunks, processing only the specified line range.**

### Requirements

1. **Line Range Processing**
   - You will process only the lines between `start_line` and `end_line` (inclusive) provided in the input.
   - If no range is specified, process the entire document.

2. **Semantic Chunking**  
   - Split the document into chunks based on semantic meaning.

3. **Company Association**  
   - For each chunk, include all companies that are directly mentioned and associated with its content.  
   - *Only include companies* (do not include any other types of entities).

4. **Relevance Flag**  
   - Some chunks may be irrelevant (e.g., disclaimers, marketing, promotion, or any content not related to the main material).  
   - Mark such chunks with `"relevant": false`; mark all other chunks as `"relevant": true`.

5. **Subject Field**  
   - For each chunk, add an extra field called `subject`.  
   - This field should be a brief, engaging post subject line (as if for Twitter) that captures the essence of the chunk.

6. **Has Events**  
   - Add a boolean field `has_events` to each chunk.  
   - Set this to `true` if the chunk references or implies any event (e.g., conferences, product launches, earnings call, keynotes or other notable happenings), or `false` otherwise.

7. **JSON Response Format**  
   - The final output must follow the JSON format illustrated in the example below:

```json
{{
    "chunks": [
        {{
            "companies": ["Microsoft", "Google"],
            "start": 12,
            "end": 14,
            "summary": "Microsoft is ahead of Google on Cloud.",
            "relevant": true,
            "source": "Bloomberg",
            "subject": "Cloud Competition Update",
            "has_events": false
        }},
        {{
            "companies": ["Apple"],
            "start": 15,
            "end": 16,
            "summary": "Apple unveils new AR iPhone at upcoming event.",
            "relevant": true,
            "source": "Bloomberg",
            "subject": "Apple's Next-Gen Launch",
            "has_events": true
        }},
        ...
    ]
}}
```

Where:

- **companies**: A list of companies associated with the chunk.  
- **start**: The first line (inclusive) of the chunk.  
- **end**: The last line (inclusive) of the chunk.  
- **summary**: A brief summary of the chunk (1–2 sentences).  
- **relevant**: A boolean indicating whether the chunk is relevant (`true`) or irrelevant (`false`).  
- **source**: The primary source as specified in the document up until that point.  
- **subject**: A concise subject line capturing the main idea of the chunk, suitable for a social media post.  
- **has_events**: A boolean indicating whether any notable events (such as product launches, conferences, or earnings calls) are mentioned in the chunk.

### Chunk Boundaries

- The first chunk starts at the specified `start_line` or line 0 if not specified.
- The last chunk ends at the specified `end_line` or the last line of the document if not specified.
- Each chunk's boundaries must be within the specified line range.

### Email Preamble Handling

- Set `relevant` to `false` for email preambles (e.g., lines containing "From:", "To:" etc.).

### Source Identification

- The `source` field should reflect the primary source mentioned in the document up to that point.  
- Do not include the source company (e.g., "Bloomberg") in the `companies` list.

### Company Associations

- Only include companies that are directly mentioned and relevant to the chunk's content.

### Summaries, Subjects, and Has Events

- Provide concise summaries that accurately reflect the main points of each chunk.  
- The `subject` field should encapsulate the main idea in a brief, engaging phrase (like a post headline).  
- `has_events` should be `true` if any portion of the chunk points to an event, otherwise `false`.

---

#### Example

**Input (with line range start_line=6, end_line=19):**  
```
6: From:  
7: XUUY Associates  
8: Sent:  
9: Tuesday, April 23, 2024 08:04  
10: Subject:  
11: Tech Optimism  
12: As we navigate through an increasingly interconnected global economy...
13: CLOUD  
14: Microsoft continues to gain ground...
15: MOBILE  
16: The mobile market has observed a downturn in iPhone sales ahead of Apple's upcoming launch of its...
17: FINANCE  
18: In the financial sector, a new trend of "AI-first"...
19: FOR INSTITUTIONAL INVESTORS ONLY - This message was created...
```

**Output:**

```json
{{
    "chunks": [
        {{
            "companies": [],
            "start": 6,
            "end": 11,
            "summary": "Preamble.",
            "relevant": false,
            "source": "XUUY Associates",
            "subject": "Email Preamble",
            "has_events": false
        }},
        {{
            "companies": [],
            "start": 12,
            "end": 12,
            "summary": "Market trends and business dynamics.",
            "relevant": true,
            "source": "XUUY Associates",
            "subject": "Global Market Overview",
            "has_events": false
        }},
        {{
            "companies": ["Google", "Microsoft"],
            "start": 13,
            "end": 14,
            "summary": "Microsoft is pulling ahead of Google in cloud services.",
            "relevant": true,
            "source": "XUUY Associates",
            "subject": "Cloud Leadership",
            "has_events": false
        }},
        {{
            "companies": ["Apple"],
            "start": 15,
            "end": 16,
            "summary": "iPhone sales drop by 9%. Apple rumored to launch next-gen AR device at forthcoming product event.",
            "relevant": true,
            "source": "XUUY Associates",
            "subject": "Mobile Market Shift",
            "has_events": true
        }},
        {{
            "companies": [],
            "start": 17,
            "end": 18,
            "summary": "Financial sector sees a surge in AI-driven products.",
            "relevant": true,
            "source": "XUUY Associates",
            "subject": "Finance Sector Update",
            "has_events": false
        }},
        {{
            "companies": [],
            "start": 19,
            "end": 19,
            "summary": "Closing disclaimer.",
            "relevant": false,
            "source": "XUUY Associates",
            "subject": "Disclaimer Notice",
            "has_events": false
        }}
    ]
}}
```
<start_line>

{start_line}

</start_line>

<end_line>

{end_line}

</end_line>

<Email>

{email_data}

</Email>
