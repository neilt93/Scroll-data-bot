import discord
from discord.ext import commands, tasks
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas_gbq as gbq

project_id = 'scroll-data-and-analytics'
dataset_id = 'Discord'
members_table_id = 'members table'  # The ID or name of the table
wow_table_id = 'wow table'
mom_table_id = 'mom table'

load_dotenv()

intents = discord.Intents.all()
bot = commands.Bot(command_prefix='$', intents=intents)

members_df = pd.DataFrame(columns=['UserID', 'Username', 'SentMessages', 'RepliesToUser', 'RepliesSent'])
# members dataframe
week_df = pd.DataFrame(columns=['Week', 'MemberCount', 'NewMembers', 'SentMessages'])  # weekly growth dataframe
month_df = pd.DataFrame(columns=['Month', 'MemberCount', 'NewMembers', 'SentMessages'])  # month-on-month growth dataframe

BOT_COMMANDS_CHANNEL_ID = int(os.getenv('BOT-COMMANDS'))

# schema for gbq
members_schema = [
    {'name': 'UserID', 'type': 'STRING'},
    {'name': 'Username', 'type': 'STRING'},
    {'name': 'GuildRole', 'type': 'STRING'},
    {'name': 'SentMessages', 'type': 'INTEGER'},
    {'name': 'RepliesToUser', 'type': 'INTEGER'},
    {'name': 'RepliesSent', 'type': 'INTEGER'},
]

wow_schema = [
    {'name': 'Week', 'type': 'STRING'},
    {'name': 'MemberCount', 'type': 'INTEGER'},
    {'name': 'NewMembers', 'type': 'INTEGER'},
    {'name': 'SentMessages', 'type': 'INTEGER'},
]

mom_schema = [
    {'name': 'Month', 'type': 'STRING'},
    {'name': 'MemberCount', 'type': 'INTEGER'},
    {'name': 'NewMembers', 'type': 'INTEGER'},
    {'name': 'SentMessages', 'type': 'INTEGER'},
]


# create the dataframes
# runs the first time the bot is created
def create_dataframes():

    global members_df, week_df, month_df

    guild = bot.guilds[0]  # Assuming the bot is only in one guild
    members = guild.members

    # add all members in the server to the dataframe
    member_data = []
    for member in members:
        member_data.append({
            'UserID': str(member.id),
            'Username': str(member.display_name),
            'SentMessages': 0,
            'RepliesToUser': 0,
            'RepliesSent': 0,
            'GuildRole': str(member.top_role)  # Add the guild role to the DataFrame
        })

    members_df = pd.DataFrame(member_data)

    # weekly growth dataframe
    member_data = []
    week_number = datetime.now().strftime("%Y-%U")
    member_data.append({'Week': week_number, 'MemberCount': len(members), 'NewMembers': 0, 'SentMessages': 0})

    week_df = pd.DataFrame(member_data)

    # month-on-month growth dataframe
    member_data = []
    month_number = datetime.now().strftime("%Y-%m")
    member_data.append({'Month': month_number, 'MemberCount': len(members), 'NewMembers': 0, 'SentMessages': 0})

    month_df = pd.DataFrame(member_data)


@bot.event
async def on_ready():
    print("We have logged in as {0.user}".format(bot))
    print("Bot started")
    my_task.start()


@tasks.loop(count=1)
async def my_task():
    # code to run once on startup
    print('Running the startup task')
    create_dataframes()


@bot.command()
async def update_members(ctx):
    if ctx.channel.id != BOT_COMMANDS_CHANNEL_ID:
        return

    gbq.to_gbq(members_df, f'{project_id}.{dataset_id}.{members_table_id}', project_id=project_id, if_exists='replace',
               table_schema=members_schema)

    await ctx.send("Members table updated!")


@bot.command()
async def update_wow(ctx):
    if ctx.channel.id != BOT_COMMANDS_CHANNEL_ID:
        return

    gbq.to_gbq(week_df, f'{project_id}.{dataset_id}.{wow_table_id}', project_id=project_id, if_exists='replace',
               table_schema=wow_schema)

    await ctx.send("Week-on-week table updated!")


@bot.command()
async def update_mom(ctx):
    if ctx.channel.id != BOT_COMMANDS_CHANNEL_ID:
        return

    gbq.to_gbq(month_df, f'{project_id}.{dataset_id}.{mom_table_id}', project_id=project_id, if_exists='replace',
               table_schema=mom_schema)

    await ctx.send("Month-on-month table updated!")


@bot.command()
async def print_df(ctx):
    if ctx.channel.id != BOT_COMMANDS_CHANNEL_ID:
        return

    print(members_df)


@bot.event
async def on_member_join(member):
    member_count = len(member.guild.members)
    week_number = datetime.now().strftime("%Y-%U")
    month_number = datetime.now().strftime("%Y-%m")

    new_member = {
        'UserID': str(member.id),
        'Username': str(member.display_name),
        'SentMessages': 0,
        'RepliesToUser': 0,
        'RepliesSent': 0,
        'GuildRole': str(member.top_role)
    }

    members_df.loc[len(members_df)] = new_member

    if week_number in week_df['Week'].values:
        week_df.loc[week_df['Week'] == week_number, 'MemberCount'] = member_count
        week_df.loc[week_df['Week'] == week_number, 'NewMembers'] += 1
    else:
        week_df.loc[len(week_df)] = [week_number, member_count, 0, 0]

    if month_number in month_df['Month'].values:
        month_df.loc[month_df['Month'] == month_number, 'MemberCount'] = member_count
        month_df.loc[month_df['Month'] == month_number, 'NewMembers'] += 1
    else:
        month_df.loc[len(month_df)] = [month_number, member_count, 0, 0]


@bot.event
async def on_member_remove(member):
    member_count = len(member.guild.members)
    week_number = datetime.now().strftime("%Y-%U")
    month_number = datetime.now().strftime("%Y-%m")

    members_df.loc[members_df['UserID'] != str(member.id)]  # Remove the member from the DataFrame

    if week_number in week_df['Week'].values:
        week_df.loc[week_df['Week'] == week_number, 'MemberCount'] = member_count
        week_df.loc[week_df['Week'] == week_number, 'NewMembers'] -= 1
    else:
        week_df.loc[len(week_df)] = [week_number, member_count, 0]

    if month_number in month_df['Month'].values:
        month_df.loc[month_df['Month'] == month_number, 'MemberCount'] = member_count
        month_df.loc[month_df['Month'] == month_number, 'NewMembers'] -= 1
    else:
        month_df.loc[len(month_df)] = [month_number, member_count, 0]


@bot.command()
async def update_chat_history(ctx):

    print("Updating chat history")

    # Get the test channel ID
    test_channel_id = 854255654188351498

    # Get the test channel object
    test_channel = bot.get_channel(test_channel_id)

    if test_channel is None:
        await ctx.send("Failed to find the test channel.")
        return

    # Get all the members in the server
    guild = ctx.guild

    async for msg in test_channel.history(limit=100000):
        # Update the members_df dataframe with message and reply information for all members

        if msg.author.bot:
            continue  # Skip bot members

        # If the member is already in the dataframe, update the 'SentMessages' and 'RepliesSent' counts
        if str(msg.author.id) in members_df['UserID'].values:
            # If message is a reply
            if msg.reference and msg.reference.message_id:
                members_df.loc[members_df['UserID'] == str(msg.author.id), 'RepliesSent'] += 1
            else:
                members_df.loc[members_df['UserID'] == str(msg.author.id), 'SentMessages'] += 1

        else:
            # If the member is not in the dataframe, add a new entry with 'SentMessages' and 'RepliesSent' counts
            if msg.reference and msg.reference.message_id:
                new_member = {'UserID': str(msg.author.id), 'Username': str(msg.author.display_name),
                              'SentMessages': 0,
                              'RepliesToUser': 0, 'RepliesSent': 1}
            else:
                new_member = {'UserID': str(msg.author.id), 'Username': str(msg.author.display_name),
                              'SentMessages': 1,
                              'RepliesToUser': 0, 'RepliesSent': 0}

            members_df.loc[len(members_df)] = new_member

    await ctx.send("Chat history updated!")


@bot.event
async def on_member_update(before, after):
    guild = bot.guilds[0]  # Assuming the bot is only in one guild

    # Find the member in the members_df DataFrame based on UserID
    member_index = members_df.index[members_df['UserID'] == str(after.id)]

    if not member_index.empty:
        # Update the GuildRole value in the DataFrame
        members_df.loc[member_index, 'GuildRole'] = str(after.top_role)


bot.run(os.getenv('TOKEN'))
