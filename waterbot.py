import os
import sys
import discord
from discord.ext import commands
import logging
import emoji
import pandas as pd
import secrets
from pint import UnitRegistry, errors
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv #This line is commented out because SparkedHost has a variables tab

load_dotenv() #This line is commented out because SparkedHost has a variables tab
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')

WTR_CHANNEL = os.getenv('WATER_CHANNEL')
WTR_GOAL = int(os.getenv('WATER_GOAL'))
WTR_FILENAME = os.getenv('WATER_QUOTES_FILENAME')

RUN_CHANNEL = os.getenv('RUNNING_CHANNEL')
RUN_GOAL = int(os.getenv('RUNNING_GOAL'))
RUN_FILENAME = os.getenv('RUNNING_QUOTES_FILENAME')

REPORT_HR = int(os.getenv('REPORT_HOUR'))
REPORT_MIN = int(os.getenv('REPORT_MINUTE'))
LOCATION = os.getenv('LOCATION')

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('discord')

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

run_channel_id = None
wtr_channel_id = None
daily_list = {}
wtr_points = {}
remaining_dist = RUN_GOAL
direction = 'up'
ureg = UnitRegistry()


cron_trigger = CronTrigger(
    hour=REPORT_HR, minute=REPORT_MIN,
    timezone=timezone(LOCATION)
)
#cron_trigger = CronTrigger(minute='*', timezone=timezone(LOCATION))


def generate_quote(filename):
    df = pd.read_csv(filename)
    random_row_idx = secrets.choice(range(df.shape[0]))
    author, quote = df.iloc[random_row_idx]
    complete_quote = f"{quote} ~ {author}"
    return complete_quote

def convert_to_km(msg_text):
    '''Uses the pint module to convert a distance measurement to km'''
    converted_dist = None

    if not msg_text:
        return converted_dist

    try:
        distance = ureg(msg_text)
        converted_dist = distance.to('km').magnitude
    except (errors.UndefinedUnitError, errors.DimensionalityError, AssertionError) as e:
        pass
    except Exception as e:
        print(f"Unexpected Error: {e}")
        
    return converted_dist

def distance_calc(remaining_dist, delta):
    '''Calculates remaining distance to 777'''
    remaining_dist = remaining_dist - delta
    if remaining_dist <= 0:
        global direction
        direction = 'down'
        remaining_dist = RUN_GOAL
        report_msg = (
            f"\U0001F3C3 You ran {delta:.1f}km"
            f"\U0001F3D4 You have reached the top of 777!\n\n"
            f"Now to get back down...\n"
            f"Remaining distance: {remaining_dist}km\n"
        )
    else:
        report_msg = (
            f"\U0001F3C3 You ran {delta:.1f}km - only {remaining_dist:.1f}km to go.\n"
            f"\U0001F3D4 Keep running {direction} that hill!\n\n"
            f"In the meantime, here is some running insight:\n"
            f"{generate_quote(RUN_FILENAME)}"
        )

    return remaining_dist, report_msg

async def scheduled_report():
    '''Triggers the report function. To be called by APScheduler'''
    logger.info("Scheduled report triggered.")
    channel = bot.get_channel(wtr_channel_id)
    message = await channel.send("Generating scheduled report...")  
    ctx = await bot.get_context(message)
    await generate_report(ctx)

@bot.event
async def on_ready():
    '''Checks initial connection and sends start-up messages.
    Defines channel IDs for later messages.'''
    global run_channel_id, wtr_channel_id

    for guild in bot.guilds:
        if guild.name == GUILD:
            break
        else:
            print('\nCheck your guild name in the env file.\n')
            sys.exit()

    print(f'{bot.user.name} is now in {guild.name}!\n')    
    
    wtr_channel = discord.utils.get(guild.channels, name=WTR_CHANNEL)
    wtr_channel_id = wtr_channel.id
    await bot.get_channel(wtr_channel_id).send("Hello water drinkers. I'm back.")
    
    run_channel = discord.utils.get(guild.channels, name=RUN_CHANNEL)
    run_channel_id = run_channel.id
    run_startup_msg = "I also help with running now. Water is essential to speed."
    await bot.get_channel(run_channel_id).send(run_startup_msg)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scheduled_report, cron_trigger)
    scheduler.start()

@bot.event
async def on_command_error(ctx, error):
    '''Handles !command errors'''
    if isinstance(error, commands.CommandError):
        response = "I don't recognise that command."
        logger.error(response+f'\n{error}\n')
        await ctx.send(response+"..\nPlease refer to '!help' for a full list.")

@bot.event
async def on_message(message):
    '''Tracks daily emoji list and total water points.
    Tracks and replies to distances posted in 777'''
    global daily_list, wtr_points, remaining_dist

    if message.channel.id == wtr_channel_id and not message.author.bot:
        msg_count = emoji.emoji_count(message.content)
        auth_idx = message.author.id
        
        if msg_count > 0:
            daily_list[auth_idx] = daily_list.get(auth_idx, 0) + msg_count
            
            if daily_list[auth_idx] >= WTR_GOAL:
                wtr_points[auth_idx] = wtr_points.get(auth_idx, 0) + 1
    
    if message.channel.id == run_channel_id and not message.author.bot:
        delta = convert_to_km(message.content.strip())
        
        if delta != None:
            remaining_dist, report_msg = distance_calc(remaining_dist, delta)
            await bot.get_channel(run_channel_id).send(report_msg)

    await bot.process_commands(message)

@bot.command(name="error", help='Tests error handling.')
async def raise_error(ctx):
    raise discord.DiscordException

@bot.command(name="report", help='Sends a report and resets daily counting.')
async def generate_report(ctx):
    global daily_list, wtr_points

    if len(daily_list) == 0:
        await ctx.send("There is no water this day.")
        return

    sorted_dly_lst = sorted(daily_list.items(), key=lambda x: x[1], reverse=True)
    sorted_wtr_pts = sorted(wtr_points.items(), key=lambda x: x[1], reverse=True)

    max_count = sorted_dly_lst[0][1]
    hydr_heroes = []
    
    report_message = "Hello water drinkers.\n"

    report_message += "\n\U0001F4A7 **Today's Results**\n"

    for user_id, count in sorted_dly_lst:
        user = await bot.fetch_user(user_id)
        report_message += f"{user.name}: {count} glass{'es'[:2*count^2]}\n"
        if count == max_count:
            hydr_heroes.append(user.name)
    
    hero_num = len(hydr_heroes)
    report_message += (
        f"\n\U0001F389 **{'** and **'.join(hydr_heroes)}** "
        f"{'are' if hero_num > 1 else 'is'} today's hydration hero{'es'[:2*hero_num^2]}!\n\n"
    )

    report_message += f"\U0001F30A **Water Points** ({WTR_GOAL} glasses a day)\n"
    for user_id, points in sorted_wtr_pts:
        user = await bot.fetch_user(user_id)
        report_message += f"{user.name}: {points} water point{'s'[:points^1]}\n"

    report_message += f"\n\U0000270D **Quote of the Day**\n"
    report_message += f"{generate_quote(WTR_FILENAME)}\n\n"

    report_message += "\U0001f4a4 It's now time for bed. I'm off to the reservoir.\n"
    daily_list = {}

    await ctx.send(report_message)

@bot.command(name="points", help="'!points username int' adds water points.")
async def mod_wtr_points(ctx, username: str, delta: int):
    global wtr_points
    user = discord.utils.get(bot.users, name=username)
    if user:
        wtr_points[int(user.id)] = wtr_points.get(int(user.id), 0) + delta
        sorted_wtr_pts = sorted(wtr_points.items(), key=lambda x: x[1], reverse=True)

        update_message = f"\U0001F30A **Updated Water Points**\n"
        for user_id, points in sorted_wtr_pts:
            user = await bot.fetch_user(user_id)
            update_message += f"{user.name}: {points} water point{'s'[:points^1]}\n"
        
        await ctx.send(update_message)

    else:
        await ctx.send(f"The user '{username}' was not found.")

@bot.command(name="quote", help="Sends a water-themed quote.")
async def extra_quote(ctx):
    report_message = "An extra quote? I'll see what I can find... \U0001F50E\n\n"
    report_message += f"{generate_quote(WTR_FILENAME)}"
    await ctx.send(report_message)

@bot.command(name="insight", help="Sends a running-themed insight.")
async def extra_insight(ctx):
    report_message = "More insights on running? I'll see what I can find... \U0001F50E\n\n"
    report_message += f"{generate_quote(RUN_FILENAME)}"
    await ctx.send(report_message)

@bot.command(name="test", help="Tests the scheduled report.")
async def test_schedule(ctx):
    await scheduled_report()

bot.run(TOKEN)