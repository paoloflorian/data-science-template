#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import pandas as pd
#########################################################################################
class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

class Timer:
    timers = {}
    dots=30 #numero di caratteri da riempire con puntini
    spaces=1  #numero di caratteri da rimepire con spazi a fine display (pulizia del testo precedente se cr=0=
    
    def __init__(
        self,
        name=None,
        logger=print,
    ):
        self._start_time = None
        self.name = name
        self.logger = logger

        # Add new named timers to dictionary of timers
        if name:
            self.timers.setdefault(name, 0)
    
    def reset_timers(self):
        '''reset timers counters '''
        timers={}
    
    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()
    
    @classmethod    
    def sec_to_hms(self,sec_elapsed):
        t_sec = round(sec_elapsed)
        (t_min, t_sec) = divmod(t_sec,60)
        (t_hour,t_min) = divmod(t_min,60) 
        return t_hour,t_min,t_sec
    
    @classmethod
    def Display(self,text_base, text_status=None, cr=1):
        """stampa il testo passato e lo allinea dx con dots, se ci sono timers in stack e non c'è text_status allora stampa anche il tempo totale"""
        if text_status:
            txt=text_base.ljust(Timer.dots, '.')+': '+text_status
            if cr:
                print(txt.ljust(Timer.spaces, ' ')) #agguingo gli spazi alla fine per pulire la riga precedente   
            else:  
                print(txt.ljust(Timer.spaces, ' '),end =  "\r") #se cr diverso da 1, non vado a capo
            Timer.spaces=len(txt) #memorizzo per pulire il prox testo stampato
        else:
            #se ho almeno un contatore allora scrivo anche la somma totale
            if Timer.timers:
                tot_elapsed=sum(list(Timer.timers.values()))
                text = 'Tempo totale'.ljust(Timer.dots, '.')+': {}h:{}m:{}s'
                print(text.format(*Timer.sec_to_hms(tot_elapsed))) #stampo il tempo totale
            ora_ini=pd.Timestamp("today").strftime('%d-%B-%Y ore %H:%M')
            print(text_base.ljust(Timer.dots, '.')+': '+ora_ini) #stampo l'ora attuale
        
    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError("Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        if self.logger:
            #self.logger(self.text.format(elapsed_time))
            text = self.name.ljust(Timer.dots, '.')+': {}h:{}m:{}s'
            self.logger(text.format(*self.sec_to_hms(elapsed_time))) #* for unpacking tuple
        if self.name:
            self.timers[self.name] += elapsed_time

        return elapsed_time
    
################################################################################

if __name__ == "__main__":
   Timer.Display('p','a'*20,cr=0)
   Timer.Display('p','b'*10,cr=0)
   Timer.Display('p','c'*5,cr=1)
   Timer.Display('p','OK')

