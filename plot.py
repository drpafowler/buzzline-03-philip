
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import csv
import matplotlib
matplotlib.use('Qt5Agg')  # Use the Qt5Agg backend

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
xs = []
ys = []

def animate(i, xs, ys):
    xs.clear()
    ys.clear()
    with open('data/data.csv', 'r') as csvfile:
        lines = csv.reader(csvfile, delimiter=',')
        for row in lines:
            x = row[0]
            y = row[1]
            xs.append(x)
            ys.append(y)
        ax.clear()
        ax.plot(xs, ys)
        plt.xlabel('Time')
        plt.ylabel('Temperature')
        plt.title('Temperature vs Time')
        plt.xticks(rotation=45)

# Create the animation and assign it to a variable
ani = animation.FuncAnimation(fig, animate, fargs=(xs, ys), interval=1000, cache_frame_data=False)

# Display the plot live
plt.show()