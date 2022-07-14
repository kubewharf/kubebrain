# Tools
This folder includes management tools which helps daily Github operations.

## Usage

### cherry-pick tools

```
export GITHUB_USER=jeffwan

# Cherry-pick pull request 2032 to branch upstream/v1.4-branch
./cherry_pick_pull.sh  upstream/v1.4-branch 2032

```

### changelog generator

> Note: Update `your_github_token` with yours.

1. Generate v1.1.0 changelog from v1.0.0
```
git log v1.0.0..v1.1.0 --oneline
```

2. Copy logs to scripts and remove tags information

```
for example: 
b52142d8 (tag: v1.1.0) chore(doc): Update README.md (#1387)

change to 
b52142d8 chore(doc): Update README.md (#1387)
```

3. Run code
```
pip3 install PyGithub
#replace <your_github_token> with your Github token
python3 changelog_generator.py
```

4. Copy the results
