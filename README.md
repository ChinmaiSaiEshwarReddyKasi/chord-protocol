# DOSP Project 3 - Chord Protocol

**Introduction:**

In this report, we describe the implementation of the chord protocol in Erlang with
mutliple nodes. In addition, we calculate the average number of hops required for
various number of actors and give our findings for each case.

**Implementation Details:**

We implemented the architecture of this project following (Section 4) of the research paper provided.

**How to Execute:**

\>c(project3).

\>project2:main(NumNodes, NumRequests)

NumNodes = number of Nodes to be created

NumRequests = number of requests to be processed

**Project Questions:**

* What is working:

  -  Creating the chord network.
  -  Joining the chord network.
  -  Stabilizing the network after join and updating the finger tables.
  -  Lookup/Searching of a key in the network.

*  Largest network created: 5000 nodes.
