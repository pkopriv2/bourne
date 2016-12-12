# Overview

Convoy is an information dissemination library - built on the SWIM [1] membership protocol.
It extends the protocol slightly to allow each member to disseminate facts about himself -
in the form a local key/value store.  Each member's store is disseminated amongs the entire
group using the same dissemination component as the membership protocol.  Because the data
replicated everywhere, each member's store should aim to be as small as possible.

For more overview and background on epidemic protocols and their analysis, please see [2].

# Intended Usage

Convoy is intended to serve as the foundation of distributed systems that require
eventually consistent, membership knowledge.  Currently, its intention is to form the
basis of a general overlay network for use in a new deployment fabric.  Stay tuned...

# Membership Overview

The heart of convoy is really just the SWIM membership protocol.  Members join, and then
serve as an information relay.  In more abstract terms, members form the vertices 
of a connected graph.  For information to spread in a atomic broadcast fashion 
(i.e. all members of the group eventually see all information), every member must propagate
to some random subset of other members. The original mathematical analysis of randomly 
connected graphs was done by Paul Erdos¨ and Alfred Renyi [2].  They concluded that
for a random graph to become fully connected, each vertex has to have at least *n ~ ln(N)* 
random edges.

To speed up the dissemation of information, convoy allows consumers to configure the number
of recipients of a message based on factors of *ln(N)*.  Due to some very specialised buffer
management a mathematical analysis of the speed of dissemination has not been done (Translation: 
Author sucks at stats)  Anyone wishing to help out with this endeavor is welcome to take a 
crack at it.  Preston is available anytime to answer any design questions you may require.  

In the meantime, here are the results of benchmarks performed locally as a function of 
cluster size and fanout factor: 

* TODO: INSERT RESULTS

## Member Health 

During each dissemination period, members are also probed for health.  Due to the expense of
being marked as failed, a member cannot be contacted, a health probe is sent to a random subset 
of members.  In this sense, each member acts as a health proxy.  If none of the members of the
health probe.

# Architecture/Design

 TODO:

# Contributors

* Preston Koprivica: pkopriv2@gmail.com
* Andy Attebery: andyatterbery@gmail.com


# References:
 [1] https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
 [2] http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf

# Other Reading:

 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
 * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
 * http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf

