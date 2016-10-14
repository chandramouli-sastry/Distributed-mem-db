import threading

from kazoo.client import KazooClient

condition = threading.Condition()


def run(num, condition):
    zk = KazooClient(hosts='127.0.0.1:2181')  # Running there
    zk.start()
    election = zk.Election("/electionpath", num)

    # blocks until the election is won, then calls
    # my_leader_function()
    # WHat is this?
    # fucked. It is happening again.
    # MY GOD..guess who i am
    x = election.run(my_leader_function, num, condition, election)
    winner = election.contenders()[0]
    print("WINNER:", winner)
    print("My x is", x, "and my num is", num)


def my_leader_function(num, condition, election):
    print("I am the leader -", num)
    global candidates
    for candidate in candidates:
        pass
    while True:
        pass


if __name__ == "__main__":
    trump = threading.Thread(target=run, args=(1, condition))
    clinton = threading.Thread(target=run, args=(2, condition))
    bernie = threading.Thread(target=run, args=(3, condition))
    zodiac = threading.Thread(target=run, args=(4, condition))  # Killer example
    candidates = [trump, clinton, bernie, zodiac]
    trump.start()
    clinton.start()
    bernie.start()
    zodiac.start()
    # Can I just while true instead of putting join? :P
    # fine. No need to give me the silent treatment
    trump.join()
    clinton.join()
    bernie.join()
    zodiac.join()
    print("I'm done. I am pointless")
