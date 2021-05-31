import discord
import random
import os

from trueskill import Rating, quality_1vs1, rate_1vs1
import pandas as pd
import numpy as np
from discord.ext import commands




# for p in sys.path:
#     print(p)

TOKEN = 'ODM3NzcxODM3Mjg1OTkwNTQx.YIxZ0w.Hy6EU6no1yKzmAw3imctip5IOA4'
client = commands.Bot(command_prefix = '!')


@client.event
async def on_ready():
    print('Bot online.')

# @client.command(brief='')
# async def command(ctx):
#     db = pd.read_csv('db.csv')
#     print('Test Command.')
#     id = ctx.message.author.id
#     await ctx.send("<@" + str(id) + ">")
#     await ctx.send(str(db))

# @client.command(brief='')
# async def db(ctx):
#     db = pd.read_csv('db.csv')
#     await ctx.send(db)

@client.command(brief='registers the user to the ladder')
async def register(ctx):
    db = pd.read_csv('db.csv')
    author = ctx.message.author
    id = ctx.message.author.id
    #await ctx.send(id)
    #await ctx.send(db.id)
    if (not id in set(db.id)):
        temp = pd.DataFrame({'name': [author], 'id': [id], 'rating': [0], 'ts': [25.0], 'stdev': [6.458], 'game wins': [0], 'game losses': [0], 'set wins': [0], 'set losses': [0]})
        temp.set_index('id')
        db = db.append(temp)
        db.to_csv('db.csv', index=False)
        #await ctx.send(f'Successfully added <@{id}> to ladder!')
        embed = discord.Embed(title=f'Successfully added <@{id}> to ladder!', color=0x00FF00)
        embed.add_field(name='id', value=id, inline=True)
        #channel = client.get_channel(ctx.channel.id)
        msg = await ctx.send(embed=embed)
    else:
        embed = discord.Embed(title="Failed to add user to ladder!", color=0xFF0000)
        embed.add_field(name='id', value=id, inline=False)
        embed.add_field(name='error', value=f"There is already a player for <@{id}> !", inline=False)
        #channel = client.get_channel(ctx.channel.id)
        msg = await ctx.send(embed=embed)
        #await ctx.send("Didn\'t create a new user for <@" + str(id) + "> becuase a user already exists!")

# @client.command(brief='report the results of a match')
# async def report(ctx, user, wins, losses):
#     return
#     db = pd.read_csv('db.csv')
#     db = db.astype({'rating': 'float64','game wins': 'int64','game losses': 'int64','set wins': 'int64','set losses': 'int64'})
#     #await ctx.send(db)
#     #await ctx.send(user)
#     id = int(user[3:-1])
#     #await ctx.send(id)
#     id1 = ctx.message.author.id
#     p1 = db[db['id'] == ctx.message.author.id].iloc[0]
#     r1 = Rating(p1.ts,p1.stdev)
#     p2 = db[db['id'] == id].iloc[0]
#     r2 = Rating(p2.ts,p2.stdev)
#     if wins > losses:
#         new_r1, new_r2 = rate_1vs1(r1,r2)
#         db = db.set_index('id')
#         db.at[id,'ts'] = new_r2.mu
#         db.at[id,'stdev'] = new_r2.sigma
#         db.at[id,'rating'] = new_r2.mu - 3.0 * new_r2.sigma
#         db.loc[id,'game wins'] += int(losses)
#         db.loc[id,'game losses'] += int(wins)
#         db.loc[id,'set losses'] += 1
#         db.at[id1,'ts'] = new_r1.mu
#         db.at[id1,'stdev'] = new_r1.sigma
#         db.at[id1,'rating'] = new_r1.mu - 3.0 * new_r1.sigma
#         db.loc[id1,'game wins'] += int(wins)
#         db.loc[id1,'game losses'] += int(losses)
#         db.loc[id1,'set wins'] += 1
#         await ctx.send(f'<@{id1}> defeated <@{id}> {wins} - {losses}')
#     elif wins < losses:
#         new_r2, new_r1 = rate_1vs1(r2,r1)
#         db = db.set_index('id')
#         db.at[id,'ts'] = new_r2.mu
#         db.at[id,'stdev'] = new_r2.sigma
#         db.at[id,'rating'] = new_r2.mu - 3.0 * new_r2.sigma
#         db.loc[id,'game wins'] += int(losses)
#         db.loc[id,'game losses'] += int(wins)
#         db.loc[id,'set wins'] += + 1
#         db.at[id1,'ts'] = new_r1.mu
#         db.at[id1,'stdev'] = new_r1.sigma
#         db.at[id1,'rating'] = new_r1.mu - 3.0 * new_r1.sigma
#         db.loc[id1,'game wins'] += int(wins)
#         db.loc[id1,'game losses'] += int(losses)
#         db.loc[id1,'set losses'] += 1
#         await ctx.send(f'<@{id}> defeated <@{id1}> {losses} - {wins}')
#     else:
#         await ctx.send('error: neither player won the set!')
#     db = db.reset_index()
#     #await ctx.send(db)
#     #await ctx.send(db.dtypes)
#     db.to_csv('db.csv', index=False)


async def print_verbose(p):
    await ctx.send(p)
    await ctx.send(p.ts)
    await ctx.send(p.stdev)

@client.command(brief='leave the queue for a match')
async def dq(ctx):
    #get the current queue
    queue = pd.read_csv('q.csv')
    #get the author's id
    id = ctx.message.author.id
    name = await client.fetch_user(id)
    #success message
    success = discord.Embed(title=f'Successfully removed {name} from queue!', color=0xFF0000)
    for i in range(0,queue.shape[0]):
        if queue.at[i,'id1'] == id and queue.at[i,'status'] == -1:
            queue.at[i,'id1'] = -1
            queue.to_csv('q.csv', index=False)
            msg = await ctx.send(embed=success)
            return
        elif queue.at[i,'id2'] != id and queue.at[i,'status'] == -1:
            queue.at[i,'id2'] = -1
            queue.to_csv('q.csv', index=False)
            msg = await ctx.send(embed=success)
            return
    failure = discord.Embed(title=f'Filed to remove {name} from queue!', color=0x00FF00)
    failure.add_field(name="Reason:",value=f'{name} wasn\'t in queue.')
    msg = await ctx.send(embed=failure)


@client.command(brief='enter the queue for a match')
async def q(ctx):
    #get the db csv
    db = pd.read_csv('db.csv')
    db = db.astype({'rating': 'float64','game wins': 'int64','game losses': 'int64','set wins': 'int64','set losses': 'int64'})
    #get the author's id
    id = ctx.message.author.id

    #get the current queue
    queue = pd.read_csv('q.csv')
    #read through queue to see if already in queue
    addable = True
    print(f'rows = {queue.shape[0]}')
    for i in range(0,queue.shape[0]):
        if (addable and (queue.at[i,'id1'] == id or queue.at[i,'id2'] == id)):
            addable = False
    print(f'addable = {addable}')
    #if we can add the user to the queue
    if (addable):
        for i in range(0,queue.shape[0]):
            if queue.at[i,'id1'] == -1 and queue.at[i,'status'] == -1:
                queue.at[i,'id1'] = id
                #if there is another player in match, start it
                if queue.at[i,'id2'] != -1:
                    queue.at[i,'status'] = 1
                    queue.to_csv('q.csv', index=False)
                    p1 = await client.fetch_user(queue.at[i,"id1"])
                    p2 = await client.fetch_user(queue.at[i,"id2"])
                    embed = discord.Embed(title=f'**{p1}** vs. **{p2}**', color=0xFF0000)
                    embed.add_field(name='How to report match: ', value="React with the Green check mark if player 1 wins and Blue check mark if player 2 wins. Both players need to report.", inline=False)
                    embed.add_field(name='How to cancel match: ', value="React with the Red X. Both players need to click the X to cancel.", inline=False)
                    embed.add_field(name='match', value='True', inline=False)
                    embed.add_field(name='player 1', value=f'<@{queue.at[i,"id1"]}>', inline=False)
                    embed.add_field(name='player 2', value=f'<@{queue.at[i,"id2"]}>', inline=False)
                    embed.add_field(name='id1', value=queue.at[i,'id1'], inline=True)
                    embed.add_field(name='id2', value=queue.at[i,'id2'], inline=True)
                    channel = client.get_channel(ctx.channel.id)
                    msg = await channel.send(embed=embed)
                    emojis = ['✅','☑️']#,'❌']
                    for emoji in emojis:
                        await msg.add_reaction(emoji)
                    return
                else:
                    embed = discord.Embed(title=f'{ctx.message.author} is looking for a match!', color=0x000000)
                    #temp ranking field
                    embed.add_field(name='Ranking', value=-1, inline=True)
                    #set id to index so we can search by id
                    db = db.set_index('id')
                    embed.add_field(name='Rating', value=round(db.at[id,'rating'],4), inline=True)
                    #await ctx.send(f"{ctx.channel.id}")
                    channel = client.get_channel(ctx.channel.id)
                    msg = await channel.send(embed=embed)
                    queue.to_csv('q.csv', index=False)
                    return

            elif queue.at[i,'id2'] == -1 and queue.at[i,'status'] == -1:
                queue.at[i,'id2'] = id
                #if there is another player in match, start it
                if queue.at[i,'id1'] != -1:
                    queue.at[i,'status'] = 1
                    queue.to_csv('q.csv', index=False)

                    p1 = await client.fetch_user(queue.at[i,"id1"])
                    p2 = await client.fetch_user(queue.at[i,"id2"])
                    embed = discord.Embed(title=f'**{p1}** vs. **{p2}**', color=0xFF0000)
                    embed.add_field(name='How to report match: ', value="React with the Green check mark if player 1 wins and Blue check mark if player 2 wins. Both players need to report.", inline=False)
                    embed.add_field(name='How to cancel match: ', value="React with the Red X. Both players need to click the X to cancel.", inline=False)
                    embed.add_field(name='match', value='True', inline=False)
                    embed.add_field(name='player 1', value=f'<@{queue.at[i,"id1"]}>', inline=True)
                    embed.add_field(name='player 2', value=f'<@{queue.at[i,"id2"]}>', inline=True)
                    embed.add_field(name='id1', value=queue.at[i,'id1'], inline=True)
                    embed.add_field(name='id2', value=queue.at[i,'id2'], inline=True)
                    channel = client.get_channel(ctx.channel.id)
                    msg = await channel.send(embed=embed)
                    emojis = ['✅','☑️','❌']
                    for emoji in emojis:
                        await msg.add_reaction(emoji)
                    return
                else:
                    embed = discord.Embed(title=f'{ctx.message.author} is looking for a match!', color=0x000000)
                    #temp ranking field
                    embed.add_field(name='Ranking', value=-1, inline=True)
                    #set id to index so we can search by id
                    db = db.set_index('id')
                    embed.add_field(name='Rating', value=round(db.at[id,'rating'],4), inline=True)
                    #await ctx.send(f"{ctx.channel.id}")
                    channel = client.get_channel(ctx.channel.id)
                    msg = await channel.send(embed=embed)
                    queue.to_csv('q.csv', index=False)
                    return

        print("no matches open")
        #we go here if there isn't an open match
        embed = discord.Embed(title=f'{ctx.message.author} is looking for a match!', color=0x000000)
        #temp ranking field
        embed.add_field(name='Ranking', value=-1, inline=True)
        #set id to index so we can search by id
        db = db.set_index('id')
        embed.add_field(name='Rating', value=round(db.at[id,'rating'],4), inline=True)
        #await ctx.send(f"{ctx.channel.id}")
        channel = client.get_channel(ctx.channel.id)
        msg = await channel.send(embed=embed)
        temp = pd.DataFrame({'id1': [id],'id2': [-1], 'status':[-1]})
        queue = queue.append(temp)
        queue.to_csv('q.csv', index=False)

        #emotes code
        #emojis = ['✅','☑️','❌']
        #for emoji in emojis:
        #    await msg.add_reaction(emoji)
    else:
        embed = discord.Embed(title=f'Error adding user to queue!', color=0xFF0000)
        embed.add_field(name='Reason:',value=f'<@{id}> is already in queue!', inline=False)
        msg = await ctx.send(embed=embed)

@client.event
async def on_reaction_add(reaction, user):
    if user.bot:
        return

    #Access embed message
    message = reaction.message
    embed = reaction.message.embeds[0]
    emoji = reaction.emoji
    #get the current queue
    queue = pd.read_csv('q.csv')


    #user_list = await message.reactions[0].users().flatten()

    ch = message.channel

    type = False
    id1 = -1
    id2 = -1

    for emb in message.embeds:
        for field in emb.fields:
            if (field.name == 'id1'):
                id1 = int(field.value)
            elif (field.name == 'id2'):
                id2 = int(field.value)
            elif (field.name == 'match'):
                type = True
            #await ch.send(field.name)
            #await ch.send(field.value)

    #await ch.send(f'id1 = {id1}')
    #await ch.send(f'id2 = {id2}')
    #await ch.send(f'type = {type}')
    #print(reaction.count)
    #print(user_list)
    if (emoji != '☑️' and emoji != '✅' and emoji != '❌') or (user.id != id1 and user.id != id2):
        await reaction.remove(user)
    else:
        #indicates that the match has already been completed
        for reaction in message.reactions:
            if reaction.emoji == '👍':
                return

        db = pd.read_csv('db.csv')
        db = db.astype({'rating': 'float64','game wins': 'int64','game losses': 'int64','set wins': 'int64','set losses': 'int64'})
        p1 = db[db['id'] == id1].iloc[0]
        r1 = Rating(p1.ts,p1.stdev)
        p2 = db[db['id'] == id2].iloc[0]
        r2 = Rating(p2.ts,p2.stdev)

        if type and emoji == '✅' and (user.id == id1 or user.id == id2):
            users = set()
            for reaction in message.reactions:
                #await ch.send(reaction.emoji)
                if reaction.emoji == '✅':
                    async for user in reaction.users():
                        users.add(user.id)

            if id1 in users and id2 in users:
                new_r1, new_r2 = rate_1vs1(r1,r2)
                db = db.set_index('id')
                db.at[id1,'ts'] = new_r2.mu
                db.at[id1,'stdev'] = new_r2.sigma
                db.at[id1,'rating'] = new_r2.mu - 3.0 * new_r2.sigma
                db.loc[id1,'set losses'] += 1
                db.at[id2,'ts'] = new_r1.mu
                db.at[id2,'stdev'] = new_r1.sigma
                db.at[id2,'rating'] = new_r1.mu - 3.0 * new_r1.sigma
                db.loc[id2,'set wins'] += 1

                # embed = discord.Embed(title=f'Error adding user to queue!', color=0xFF0000)
                # embed.add_field(name='Reason:',value=f'<@{id}> is already in queue!', inline=False)
                # msg = await ctx.send(embed=embed)

                await ch.send(f'<@{id1}> wins!')
                await message.add_reaction('👍')
                #update queue
                queue = queue[queue['id1'] != id1]
                queue.to_csv('q.csv', index=False)
                #update db
                db = db.reset_index()
                db.to_csv('db.csv', index=False)
            #await ch.send(users)
            #await fixed_channel.send(embed=embed)
        elif type and emoji == '☑️' and (user.id == id1 or user.id == id2):
            users = set()
            for reaction in message.reactions:
                #await ch.send(reaction.emoji)
                if reaction.emoji == '☑️':
                    async for user in reaction.users():
                        users.add(user.id)
            if id1 in users and id2 in users:
                new_r2, new_r1 = rate_1vs1(r2,r1)
                db = db.set_index('id')
                db.at[id1,'ts'] = new_r1.mu
                db.at[id1,'stdev'] = new_r1.sigma
                db.at[id1,'rating'] = new_r1.mu - 3.0 * new_r1.sigma
                db.loc[id1,'set losses'] += 1
                db.at[id2,'ts'] = new_r2.mu
                db.at[id2,'stdev'] = new_r2.sigma
                db.at[id2,'rating'] = new_r2.mu - 3.0 * new_r2.sigma
                db.loc[id2,'set wins'] += 1

                await ch.send(f'<@{id2}> wins!')
                await message.add_reaction('👍')
                #update queue
                queue = queue[queue['id1'] != id1]
                queue.to_csv('q.csv', index=False)
                #update db
                db = db.reset_index()
                db.to_csv('db.csv', index=False)

        elif type and emoji == '❌' and (user.id == id1 or user.id == id2):
            users = set()
            for reaction in message.reactions:
                #await ch.send(reaction.emoji)
                if reaction.emoji == '❌':
                    async for user in reaction.users():
                        users.add(user.id)
            if id1 in users and id2 in users:
                await ch.send(f'Cancelled the match!')
                await message.add_reaction('👍')
                queue = queue[queue['id1'] != id1]
                queue.to_csv('q.csv', index=False)

@client.command(brief='showing rankings')
async def rank(ctx):
    #get the db csv
    db = pd.read_csv('db.csv')
    db = db.astype({'rating': 'float64','game wins': 'int64','game losses': 'int64','set wins': 'int64','set losses': 'int64'})

    db = db.sort_values(by=['rating'], ascending=False)
    db = db.reset_index()
    print(db)
    embed = discord.Embed(title=f'Rankings', color=0xFFFF00)
    for i in range(0,db.shape[0]):
        embed.add_field(name=f'{i + 1}', value=f"{db.at[i,'name']}:\n rating: {round(db.at[i,'rating'],4)} \n uncertainty: {round(db.at[i,'stdev'],4)}", inline=False)
    msg = await ctx.send(embed=embed)

@client.command(brief='admin command - stops ladderer')
async def stop(ctx):
    if ctx.message.author.id == 203624088420352001 or ctx.message.author.id == 837794320953507840:
        await ctx.send("Stopped Ladderer")
        await client.logout()
    else:
        await ctx.send("You don\'t have permission to use this command ")

@client.command(brief='admin command - clears the queue completely')
async def cq(ctx):
    if ctx.message.author.id == 203624088420352001 or ctx.message.author.id == 837794320953507840:
        queue = pd.DataFrame({'id1': [-1],'id2': [-1], 'status':[-1]})
        queue.to_csv('q.csv', index=False)
        await ctx.send("cleared queue.")
    else:
        await ctx.send("You don\'t have permission to use this command ")

client.run(TOKEN)