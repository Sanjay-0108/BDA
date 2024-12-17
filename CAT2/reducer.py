#!/usr/bin/env python3

import sys

from collections import defaultdict



# Data structures

user_totals = defaultdict(lambda: {'sum': 0, 'count': 0})

transactions = []  # Store transactions for anomaly detection



# Step 1: Read input from the mapper

for line in sys.stdin:

    line = line.strip()

    parts = line.split("\t")

    if len(parts) == 3:  # user_id, transaction_id, amount

        user_id, transaction_id, amount = parts

        try:

            amount = float(amount)

            # Update totals for average calculation

            user_totals[user_id]['sum'] += amount

            user_totals[user_id]['count'] += 1

            # Store transaction for anomaly detection

            transactions.append((user_id, transaction_id, amount))

        except ValueError:

            continue



# Step 2: Calculate average for each user

user_avg = {user: totals['sum'] / totals['count'] for user, totals in user_totals.items()}



# Step 3: Output results

print("a) Average transaction amount per user:\n")

for user, avg in user_avg.items():

    print(f"{user}\t{avg:.2f}")



print("\n b) Anomalies:")

for user_id, transaction_id, amount in transactions:

    if amount > 2 * user_avg[user_id]:

        print(f"{transaction_id}\t{user_id}\t{amount:.2f} ANOMALY")

