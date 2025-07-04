import asyncio
import functools
import ipywidgets as ipw
from datetime import datetime
from utils.control import * 
from utils.aiida_and_ssh_utils import key_is_valid,get_old_unfinished_workchains
__version__ = "v2025.0214"

class ConfigAiiDAlabApp(ipw.VBox): 
    def __init__(self):
        self.title = ipw.HTML("<h2>Config AiiDAlab Application</h2>")

        style = {'description_width': '150px'}  # Adjust as needed
        
        self.update_message = ipw.HTML("Nothing to report")
        self.update_old_workchains = ipw.HTML("")
        self.running_workchains = ipw.HTML("")
        self.paused_workchains = ipw.HTML("")
        self.check = True # set to False while applying updates and then set to True again

        # Check for updates button
        self.check_button = ipw.Button(description="Inspect updates", button_style="info")
        self.check_button.on_click(self.check_for_all_updates)
        # Start button
        self.start_button = ipw.Button(description="Apply updates", button_style="primary",disabled=True)
        self.start_button.on_click(self.run_configuration)

        # Clear button
        self.clear_button = ipw.Button(description="Clear logs", button_style="warning") 
        self.clear_button.on_click(self.clear_output)
        
        # Play paused workchains button
        self.play_button = ipw.Button(description="Play paused workchains", button_style="success",disabled=True)
        self.play_button.on_click(self.play_paused)

        # Output display
        self.subtitle = ipw.HTML("")
        self.output = ipw.Output()
        
        # initialize widgets and variables
        self.config_widgets = self.widgets_from_yaml()
        some_paused_calculations,self.paused_calculations = self.check_paused_workchains()
        self.play_button.disabled = not some_paused_calculations
        
        # Call VBox constructor directly
        super().__init__([
            self.title,
            self.update_old_workchains,  # Display updates for old workchains
            self.running_workchains,  # Display running workchains
            self.paused_workchains,  # Display paused workchains
            self.update_message,  # Display general updates
            ipw.HBox([widget for widget in self.config_widgets.values()]),
            ipw.HBox([self.check_button,self.start_button, self.play_button, self.clear_button]),
            self.subtitle,
            self.output
        ])

        # Start periodic checks
        #asyncio.create_task(self._start_periodic_check_updates(60))
        #asyncio.create_task(self._start_periodic_check_old_workchains(60))
        
    async def _start_periodic_check_updates(self, interval):
        """Periodically check for updates."""
        while True:
            if self.check:
                msg,self.updates_needed,self.config = await asyncio.to_thread(functools.partial(check_for_updates,self.account_widget.value))
                all_grants = ['']+[grant for grants_list in self.config["grants"].values() for grant in grants_list]
                self.account_widget.options = all_grants
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = remove_green_check_lines(msg)
                if not msg:
                    msg = "✅ Nothing to report"
                self.update_message.value = f"<b>{timestamp}</b>: {remove_green_check_lines(msg)}"
            await asyncio.sleep(interval)

    async def _start_periodic_check_old_workchains(self, interval):
        """Periodically check for pending too old workchains."""
        while True:
            if self.check:
                update_result = await asyncio.to_thread(get_old_unfinished_workchains)
                self.update_old_workchains.value = f"<b>Old WorkChains Check:</b> {update_result}"
            await asyncio.sleep(interval)
            
                
    def widgets_from_yaml(self,file_path='/home/jovyan/opt/aiidalab-alps-files/config.yml'):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_ok,msg = check_repository()
        if not status_ok:
            self.update_message.value = f"<b>{timestamp}</b>: ❌ Repository is not cloned"
            return None
        self.update_message.value = f"<b>{timestamp}</b>: {msg}"
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Get widgets from the yaml file
        yaml_widgets = data.get('widgets', {})
        
        # Create dropdown widgets form teh yamls file
        widgets = {key: ipw.Dropdown(description=key, options=options) for key, options in yaml_widgets.items()}
        return widgets        
    
    def check_paused_workchains(self):
        """Check for paused workchains."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        some_paused,msg = get_old_unfinished_workchains(cutoffdays=4,reverse=True,paused=True)
        if msg !='':
            self.paused_workchains.value = f"<b>{timestamp}</b>: There are paused workchains: {msg}"
        return some_paused,msg
    
    def play_paused(self,_):
        self.play_button.disabled = True
        print(f"AAAAAAA{self.paused_calculations}")
        if self.paused_calculations != '':
            output,success = play_paused_workchains(self.paused_calculations)
        if success:
            self.paused_workchains.value = f"<b>{output}</b>: Workchains are resumed"
        else:
            self.paused_workchains.value = f"<b>{output}</b>: Workchains are not resumed, please check"

    def check_for_all_updates(self,_):
        status_ok,msg,self.config = get_config(config_widgets=self.config_widgets)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not status_ok:
            self.update_message.value = f"<b>{timestamp}</b>: {msg}"
            return
        ssh_key_updated = key_is_valid(public_key_file=self.config['variables']['ssh_public_key'])
        if not ssh_key_updated:
            self.update_message.value = f"<b>{timestamp}</b>: ❌ SSH key is not valid, please update it"
            return
        if msg =='':
            msg,self.updates_needed = check_for_updates(self.config,self.config_widgets['grant'].value)        
        msg = remove_green_check_lines(msg)
        if not msg:
            self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report" 
        else:
            self.update_message.value = f"<b>{timestamp}</b>: {remove_green_check_lines(msg)}"   
        # check for zombie workcains  
        someoldzombie,msg = get_old_unfinished_workchains()
        self.update_old_workchains.value = f"<b>Old WorkChains Check:</b> {msg}"
        somerunning,msg = get_old_unfinished_workchains(cutoffdays=3,reverse=True)
        if somerunning:
            self.running_workchains.value = f"<b>There are running workchains, you cannot update:</b> {msg}"
        else:
            self.start_button.disabled = False   
        
    def clear_output(self,_):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.output.clear_output()
        self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report"
        self.subtitle.value = ""
        self.paused_workchains.value = ""
        self.start_button.disabled = True
      
    def run_configuration(self,_):
        self.check = False
        self.output.clear_output()
        self.subtitle.value = "<h3>Cloning repository with config files</h3>"
            
        #self.output.clear_output()        
        self.subtitle.value = "<h3>Setup SSH config file. Check SSH connection.</h3>"
        with self.output:
            #self.output.clear_output()
            if "ssh_config" in self.updates_needed:
                update_ssh_config(config_path,self.config['ssh_config'],rename=self.updates_needed['ssh_config']['rename'])
                if not set_ssh(self.config['computers'],self.updates_needed['ssh_config']['hosts']):
                    print("❌ ssh problem, ask for support")
                    return
            print("✅ ssh setup done")
            
        #self.output.clear_output()
        self.subtitle.value = "<h3>Setup computers</h3>"
        with self.output:    
            print("🔄 Setting up computers")
            status = setup_computers(self.updates_needed.get('computers',{}),self.config['computers'])
            if not status:
                return
            print("✅ Done")
        #self.output.clear_output()
        self.subtitle.value = "<h3>Setup Codes and Uenvs. It will take several minutes</h3>"
        with self.output:
            # setup codes and uenvs
            print("🔄 Setting up codes")
            status,uenvs = setup_codes(self.updates_needed.get('codes',{}),self.config)
            if len(uenvs) >0:
                uenvs_ok = manage_uenv_images(uenvs)
                if not uenvs_ok:
                    print("❌ uenvs not set up correctly ask for help")
                    return
            print("✅ Done")
        self.subtitle.value = "<h3>Additional commands</h3>" 
        with self.output:
            # setup codes and uenvs
            print("🔄 Executing final commands")  
            status_ok = execute_custom_commands(self.config)
            if not status_ok:
                print("❌ custom commands not set up correctly ask for help")
                return
            print("✅ Done") 
        self.start_button.disabled = True
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report" 
        return

# Example function
def get_start_widget(appbase, jupbase, notebase):
    return ConfigAiiDAlabApp()  # ✅ Return instance of the class

