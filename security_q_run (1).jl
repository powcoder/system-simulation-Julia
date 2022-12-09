include("security_q.jl")

# inititialise
seed = 1
T = 10_000.0
mean_interarrival = 5.2 
n_queues = 2
mean_server_time = 10.0
# n_queues = 1
# mean_server_time = 5.0 # halve the server time when there is only one queue
time_units = "minutes"
P = Parameters( seed, T, n_queues, mean_interarrival, mean_server_time, time_units)

# file directory and name; * concatenates strings.
dir = pwd()*"/data/"*"/seed"*string(P.seed)*"/n_queues"*string(P.n_queues) # directory name
mkpath(dir)                          # this creates the directory 
file_entities = dir*"/entities.csv"  # the name of the data file (informative) 
file_state = dir*"/state.csv"        # the name of the data file (informative) 
fid_entities = open(file_entities, "w") # open the file for writing
fid_state = open(file_state, "w")       # open the file for writing

write_metadata( fid_entities )
write_metadata( fid_state )
write_parameters( fid_entities, P )
write_parameters( fid_state, P )

# headers
write_entity_header( fid_entities,  Customer(0, 0.0) )
print(fid_state,"time,event_id,event_type,timing,length_event_list")
for i=1:n_queues
    print(fid_state,",length_queue$(i),in_service$(i)")
end
println(fid_state)

# run the actual simulation
(system,R) = initialise( P ) 
run!( system, P, R, fid_state, fid_entities)

# remember to close the files
close( fid_entities )
close( fid_state )
