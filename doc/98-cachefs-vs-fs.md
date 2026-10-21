object store cache fs is different:
1. values never change. only insert or remove. 
2. consistency: you can lose value, you can lose partial value, but you can't have incoherent value.
3. conventional fs is designed when file system is mostly not full, and slows down significantly when it's about full; cache fs is operated when it is ALWAYS full.
4. each access is at least 512KB (typically 1MB), contiguity is unnecessary. 
