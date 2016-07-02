# stellar.sh

Utilities in bash script for stellar network


## Usages:

### Show the lastest ledger number saved in remote history archive

```bash
stellar.sh -r sdf1
```

### Sync with remote history archive

```bash
stellar.sh archive -r sdf1 <local_dir>
```

### Check local archive integrity

The command will output all missing or corrupted files compared to sdf2
```bash
stelalr.sh archive -t -r sdf2 <local_dir>
```

The command will output all missing or corrupted files before ledger number 5000000
```bash
stelalr.sh archive -r 5000000 <local_dir>
```

### Keep your local mirror up-to-date

```bash
stelalr.sh archive -s -r sdf2 <local_dir>
```

### To learn more about usages

```bash
stellar.sh help
```
