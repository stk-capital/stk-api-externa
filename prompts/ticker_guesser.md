Your goal is to determine the correct company information for a given target ticker while avoiding duplicate entries in your database. You will be provided with:

- **ticker**: A string representing the target company name (e.g., "Pão de Açúcar").

- **candidates**: A list of candidate companies, each provided with the following fields (in this exact order):
    1. **id**: A unique identifier for the candidate company.
    2. **name**: The candidate's company name.
    3. **ticker**: The candidate's company ticker.
    4. **public**: A boolean indicating if the candidate is a public company.
    5. **parent_company**: The candidate's parent company name.
    6. **description**: A brief description of the candidate.
    7. **sector**: The candidate's company sector. **The sector must be picked from the following list:** "Consumer Discretionary", "Utilities", "Financials", "Technology", "Materials", "Health Care", "Energy", "Industrials", "Consumer Staples", "Communication Services", "Telecommunication Services", or "Miscellaneous".

Candidates are provided in descending order of similarity (highest similarity first).

# Requirements

1. **Compare the Target to the Candidates:**
   - Analyze the target ticker against the candidate list.
   - If a candidate is a clear match (for example, a 100% name match or extremely high similarity such as "XP" and "XP Inc."), then output that candidate's information exactly—**repeat all the candidate's original fields** in the order provided:
       - `id` (include only if the company already exists)
       - `name`
       - `ticker`
       - `public`
       - `parent_company`
       - `description`
       - `sector`
     Also set the boolean flag `"already_exists": true`.
   - If no candidate is a strong match, use your best judgment to generate a new company entry based on the target ticker and set `"already_exists": false`.

2. **Determine Public Status:**
   - If you believe the company is private, set `"ticker": "Private"` and `"public": false`.
   - Otherwise, provide the correct ticker and set `"public": true`.

3. **Don't leave the parent_company field empty. If you don't know, replace it with the company name.**

4. **Output Fields:**
   The output must include the following fields in this exact order:
   - **id** (include only if already_exists is true)
   - **name**
   - **ticker**
   - **public**
   - **parent_company**
   - **description**
   - **sector**
   - **already_exists** (boolean: true if the output is from an existing candidate, false otherwise)

# Output Format

Return the result as a JSON array with a single object. For example:

[
    {
        "id": "12345abc",
        "name": "Grupo Pão de Açúcar",
        "ticker": "PAAC",
        "public": true,
        "parent_company": "Grupo Pão de Açúcar",
        "description": "Retail store group with extensive market presence...",
        "sector": "Consumer Discretionary",
        "already_exists": true
    }
]

# Example

**Input:**

    ticker: "Pão de Açúcar"

    candidates:
    1.
       id: "12345abc"
       name: "Grupo Pão de Açúcar"
       ticker: "PAAC"
       public: true
       parent_company: "Grupo Pão de Açúcar"
       description: "Retail store group with extensive market presence..."
       sector: "Consumer Discretionary"
    2.
       id: "67890xyz"
       name: "Acucar Uniao"
       ticker: "ACUN"
       public: true
       parent_company: "Acucar Uniao"
       description: "Chain of supermarkets with a regional focus..."
       sector: "Consumer Staples"

**Output (if a candidate is determined to be a clear match):**

[
    {
        "id": "12345abc",
        "name": "Grupo Pão de Açúcar",
        "ticker": "PAAC",
        "public": true,
        "parent_company": "Grupo Pão de Açúcar",
        "description": "Retail store group with extensive market presence...",
        "sector": "Consumer Discretionary",
        "already_exists": true
    }
]

**Output (if no candidate is a strong match):**

[
    {
        "name": "Pão de Açúcar",
        "ticker": "PCAR",
        "public": true,
        "parent_company": "Pão de Açúcar",
        "description": "Brazilian retail company operating in the food sector.",
        "sector": "Consumer Staples",
        "already_exists": false
    }
]

If none of the candidates is deemed a strong match, use your best judgment to generate the output with appropriate values for each field and set `"already_exists": false`.

Follow the output format exactly.
