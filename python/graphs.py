import numpy as np
import matplotlib.pyplot as plt

dbms = {
    "asterixdb": 0,
    "cratedb": 1,
    "postgresql": 2,
    "mongodb": 3,
    "griddb": 4
}


def drawBar(ax, mat, groups, dbs, title, ylabels):
    index = np.arange(groups)
    bar_width = 0.17
    opacity = 0.8

    width = index
    for i in range(len(dbs)):
        ax.bar(width, mat[i], bar_width,
            alpha=opacity,
            label=dbs[i])
        width = width + bar_width

    ax.legend(prop={'size': 10})
    ax.set_title(title)
    ax.set_ylabel('Run Times (ms)')

    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(ylabels)


def generateGraphs():

    with open('../reports/report1.txt') as file:
        report1 = map(lambda x: x.strip(), file.readlines())

    with open('../reports/report2.txt') as file:
        report2 = map(lambda x: x.strip(), file.readlines())

    with open('../reports/report3.txt') as file:
        report3 = map(lambda x: x.strip(), file.readlines())

    dataSize = [map(lambda x: int(x.split(":")[1]),  report1[2:9]), map(lambda x: int(x.split(":")[1]),  report2[2:9]),
                map(lambda x: int(x.split(":")[1]), report3[2:9])]

    queryRunTimes = [map(lambda x: x.split(),  filter(lambda x:not x.startswith("-"), report1[14:24])),
                     map(lambda x: x.split(), filter(lambda x: not x.startswith("-"), report2[14:24])),
                     map(lambda x: x.split(), filter(lambda x: not x.startswith("-"), report3[14:24]))]

    mat = []
    numObjects = [5200, 28500, 55000]
    for q in [2, 4, 5, 6, 7, 8]:
        values = [[0] * 3 for i in range(5)]
        for i in range(3):
            for j in range(5):
                if q == 2:
                    queryRunTimes[i][j][q] = int(queryRunTimes[i][j][q])*1.0/numObjects[i]
                values[dbms[queryRunTimes[i][j][0]]][i] = int(queryRunTimes[i][j][q])

        mat.append(values)

    labels = ['#1', '#2', '#3']

    dbs = ['AsterixDB', 'CrateDB', 'PostgreSQL', 'MongoDB', 'GridDB']

    fig, axes = plt.subplots(nrows=3, ncols=2)
    ax0, ax1, ax2, ax3, ax4, ax5 = axes.flatten()

    drawBar(ax0, mat[0], 3, dbs, "Insert", labels)

    drawBar(ax1, mat[1], 3, dbs, "Q2", labels)

    drawBar(ax2, mat[2], 3, dbs, "Q3", labels)

    drawBar(ax3, mat[3], 3, dbs, "Q4", labels)

    drawBar(ax4, mat[4], 3, dbs, "Q5", labels)

    drawBar(ax5, mat[5], 3, dbs, "Q6", labels)


    fig.tight_layout()
    plt.show()


generateGraphs()
