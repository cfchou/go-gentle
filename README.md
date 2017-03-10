
##Go-gentle
Talk to external services like a gentleman.




##Appendix

###state machine of circuits

```
Two rolling metrics are relevent:
reqs_last_10s := metrics.requests.take(-10s)
err_last_10s := metrics.errors.take(-10s) 
```

A circuit, according to its state, may allow or disallow requests.

Requests, if they are given the green light to run, would update the metrics.
If they are disallow, no update to metrics including __reqs_last_10s__.


```
[closed] -> [open] when:
reqs_last_10s > RequestVolumeThreshold &&
    err_last_10s / req_last_10s > ErrorPercentThreshold
```

```
[open] -> [half_closed] -> [closed] when:
now.Sub(openedOrLastTestTime) > SleepWindow && reqest.success
    
```
