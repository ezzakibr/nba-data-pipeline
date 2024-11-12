import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
import seaborn as sns

# Read the data
df = pd.read_csv('nba_data.csv')

# Create the dashboard
plt.figure(figsize=(20, 12))
gs = gridspec.GridSpec(2, 2)
gs.update(wspace=0.3, hspace=0.4)

# Dashboard title
plt.suptitle('NBA Performance Dashboard', fontsize=20, y=0.95)

# 1. Team Performance Metrics (Top Left)
ax1 = plt.subplot(gs[0, 0])
team_stats = df.groupby('team_name_1').agg({
    'game_points': 'mean',
    'total_reb': 'mean',
    'total_ast': 'mean',
    'total_blk': 'mean'
}).round(1)

x = np.arange(len(team_stats.index))
width = 0.2

ax1.bar(x - width*1.5, team_stats['game_points'], width, label='Points', color='#3498db')
ax1.bar(x - width/2, team_stats['total_reb'], width, label='Rebounds', color='#e74c3c')
ax1.bar(x + width/2, team_stats['total_ast'], width, label='Assists', color='#2ecc71')
ax1.bar(x + width*1.5, team_stats['total_blk'], width, label='Blocks', color='#f1c40f')

ax1.set_ylabel('Average per Game')
ax1.set_title('Team Performance Metrics')
ax1.set_xticks(x)
ax1.set_xticklabels(team_stats.index, rotation=45)
ax1.legend()
ax1.grid(True, alpha=0.3)

# 2. Top Scorers (Top Right)
ax2 = plt.subplot(gs[0, 1])
scorer_freq = df['best_scorer'].value_counts().head()
y_pos = np.arange(len(scorer_freq))
ax2.barh(y_pos, scorer_freq.values, color='purple', alpha=0.6)
ax2.set_yticks(y_pos)
ax2.set_yticklabels(scorer_freq.index)
ax2.set_xlabel('Number of Games as Best Scorer')
ax2.set_title('Top 5 Best Scorers by Frequency')
ax2.grid(True, alpha=0.3)

# 3. Points Distribution (Bottom Left)
ax3 = plt.subplot(gs[1, 0])
sns.boxplot(x='team_name_1', y='game_points', data=df, ax=ax3)
ax3.set_xticklabels(ax3.get_xticklabels(), rotation=45)
ax3.set_title('Points Distribution by Team')
ax3.grid(True, alpha=0.3)

# 4. Player Impact Distribution (Bottom Right)
ax4 = plt.subplot(gs[1, 1])

# Get overall player impact by combining all categories
all_players = pd.concat([
    df['best_scorer'].value_counts(),
    df['best_rebounder'].value_counts(),
    df['best_assist'].value_counts(),
    df['best_blocker'].value_counts()
], axis=1)

all_players.columns = ['Scoring', 'Rebounding', 'Assists', 'Blocks']
all_players = all_players.fillna(0)

# Calculate total impact and sort
all_players['Total'] = all_players.sum(axis=1)
top_players = all_players.nlargest(8, 'Total')  # Get top 8 most impactful players
top_players = top_players.drop('Total', axis=1)

# Create stacked bar chart for top players
bottom = np.zeros(len(top_players))
categories = ['Scoring', 'Rebounding', 'Assists', 'Blocks']
colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99']

for i, category in enumerate(categories):
    ax4.bar(top_players.index, top_players[category], bottom=bottom, 
            label=category, color=colors[i], alpha=0.7)
    bottom += top_players[category]

ax4.set_title('Top Players Multi-Category Impact')
ax4.set_xlabel('Players')
ax4.set_ylabel('Number of Games as Best Performer')
ax4.legend()
ax4.tick_params(axis='x', rotation=45)
ax4.grid(True, alpha=0.3)

# Add total numbers on top of each bar
for i, player in enumerate(top_players.index):
    total = top_players.loc[player].sum()
    ax4.text(i, total + 0.5, f'Total: {int(total)}', 
             ha='center', va='bottom')

plt.tight_layout()
plt.show()

# Print detailed breakdown
print("\nDetailed Player Impact Breakdown:")
print(top_players.round(2))
print("\nSelection Criteria: Players with highest total appearances as best performer across all categories")