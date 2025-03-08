import asyncio
import functools
import ipywidgets as ipw
from datetime import datetime
from utils.control import * 
__version__ = "v2025.0214"

class ConfigAiiDAlabApp(ipw.VBox): 
    def __init__(self):
        self.title = ipw.HTML("<h2>Config AiiDAlab Application</h2>")

        style = {'description_width': '150px'}  # Adjust as needed

        # Create input fields with adjusted description width
        self.username_widget = ipw.Text(
            value='',
            description='CSCS Username:',
            placeholder='Enter your username',
            layout=ipw.Layout(width='400px'),  # Total width
            style=style
        )

        self.account_widget = ipw.Dropdown(
            value='',
            options=[''],
            description='CSCS Account:',
            layout=ipw.Layout(width='400px'),
            style=style
        )
        
        self.update_message = ipw.HTML("Nothing to report")
        self.update_old_workchains = ipw.HTML("")
        self.check = True # set to False while applying updates and then set to True again

        # Checkbox for QE Postprocess
        self.qe_postprocess_checkbox = ipw.Checkbox(value=False, description="QE Postprocess")

        # Start button
        self.start_button = ipw.Button(description="Start", button_style="primary")
        self.start_button.on_click(self.run_configuration)

        # Clear button
        self.clear_button = ipw.Button(description="Clear", button_style="warning") 
        self.clear_button.on_click(self.clear_output)

        # Output display
        self.subtitle = ipw.HTML("")
        self.output = ipw.Output()

        # Call VBox constructor directly
        super().__init__([
            self.title,
            self.update_old_workchains,  # Display updates for old workchains
            self.update_message,  # Display general updates
            self.username_widget,
            self.account_widget,
            self.qe_postprocess_checkbox,
            ipw.HBox([self.start_button, self.clear_button]),
            self.subtitle,
            self.output
        ])

        # Start periodic checks
        asyncio.create_task(self._start_periodic_check_updates(60))
        asyncio.create_task(self._start_periodic_check_old_workchains(60))
        
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
                   
    def clear_output(self,_):
        self.output.clear_output()
        self.subtitle.value = ""
      
    def run_configuration(self,_):
        self.check = False
        self.output.clear_output()
        self.subtitle.value = "<h3>Cloning repository with config files</h3>"
        with self.output:        
            if self.username_widget.value == '':
                print("❌ Specify the user")
                return
        cscs_username = self.username_widget.value
            
        #self.output.clear_output()        
        self.subtitle.value = "<h3>Setup SSH config file. Check SSH connection.</h3>"
        with self.output:
            #self.output.clear_output()
            if "ssh_config" in self.updates_needed:
                update_ssh_config(config_path,self.config['ssh_config'],self.username_widget.value,rename=self.updates_needed['ssh_config']['rename'])
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
            qe_uenv = next((env[1] for env in uenvs if 'espresso' in env[1]), None)
            if len(uenvs) >0:
                uenvs_ok = manage_uenv_images(uenvs)
                if not uenvs_ok:
                    print("❌ uenvs not set up correctly ask for help")
                    return
            print("✅ Done")    
        if self.qe_postprocess_checkbox.value:
            
            computer = self.config['codes']['pw']['computer']
            remotehost = self.config['computers'][computer]['setup']['hostname']
            #self.output.clear_output()
            self.subtitle.value = "<h3>Setup Phonopy and Critic2</h3>"
            with self.output:
                print("🔄 Setting up phonopy")
                phonopy_ok = setup_phonopy(cscs_username,remotehost)
                if not phonopy_ok:
                    print("❌ phonopy not set up correctly ask for help")
                    return
                print("✅ phonopy setup done")
            self.output.clear_output()
            self.subtitle.value = "<h3>Setup Critic2</h3>"  
            with self.output:
                print("🔄 Creating conda environment will take a while")
                match = re.search(r"#SBATCH --uenv=([\w\-/.]+:\d+)", self.config['codes']['pw']['prepend_text'])
                qe_uenv = match.group(1) if match else None
                critic2_ok = setup_critic2(cscs_username,qe_uenv,remotehost,self.config['python_version'])
                if not critic2_ok:
                    print("❌ critic2 not set up correctly ask for help")
                    return
                print("✅ critic2 setup done")
        self.check = True
        return

# Example function
def get_start_widget(appbase, jupbase, notebase):
    return ConfigAiiDAlabApp()  # ✅ Return instance of the class

