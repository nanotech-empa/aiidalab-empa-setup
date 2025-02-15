import asyncio
import ipywidgets as ipw
from datetime import datetime
from utils.config import * 
__version__ = "v2025.0214"

class ConfigAiiDAlabApp(ipw.VBox):  # ✅ Correct inheritance
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
            value='s1267',
            options=['s1267', 's1276'],
            description='CSCS Account:',
            layout=ipw.Layout(width='400px'),
            style=style
        )
        
        self.update_message = ipw.HTML("Nothing to report")

        # Checkbox for QE Postprocess
        self.qe_postprocess_checkbox = ipw.Checkbox(value=False, description="QE Postprocess")

        # Start button
        self.start_button = ipw.Button(description="Start", button_style="primary")
        self.start_button.on_click(self.run_configuration)
        # Clear button
        self.clear_button = ipw.Button(description="Clear", button_style="warning") 
        self.clear_button.on_click(self.clear_output)
        

        # Output display
        self.subtitle=ipw.HTML("")
        self.output = ipw.Output()
        

        # ✅ Call VBox constructor directly instead of assigning self.layout
        super().__init__([
            self.title,
            self.update_message,
            self.username_widget,
            self.account_widget,
            self.qe_postprocess_checkbox,
            ipw.HBox([self.start_button,self.clear_button]),
            self.subtitle,
            self.output
        ])
        asyncio.ensure_future(self._start_periodic_check(3))
        
    async def _start_periodic_check(self, interval):
        """Periodically check for updates."""
        while True:
            update_result = await asyncio.to_thread(check_for_updates)
            self.update_message.value = datetime.now().strftime("%Y-%m-%d %H:%M:%S") +'  '+ update_result
            await asyncio.sleep(interval)
                   
    def clear_output(self,_):
        self.output.clear_output()
        self.subtitle.value = ""
      
    def run_configuration(self,_):
        self.output.clear_output()
        self.subtitle.value = "<h3>Cloning repository with config files</h3>"
        with self.output:        
            if self.username_widget.value == '':
                print("❌ Specify the user")
                return
            cscs_username = self.username_widget.value
            cscs_account = self.account_widget.value
            # clone repository
            clone_repo(home_dir,repo_name)
        self.output.clear_output()
        self.subtitle.value = "<h3>Check AiiDA daint.alps computer</h3>"
        with self.output:
            # set username in yml files
            update_yml_files(cscs_username, cscs_account, alps_files, yml_and_config_files)
            
            # check if we have to install teh new computer
            need_to_install = check_install_computer()
            if need_to_install:
                setup_new_alps()
            else:
                print("✅ Skipping installation of new computer and the installed computer does not need to be relabeled")
            run_command(config_command(cscs_username),ssh=False)
        self.output.clear_output()
        self.subtitle.value = "<h3>Check/Setup SSH </h3>"
        with self.output:
            if not set_ssh(cscs_username):
                print("❌ ssh problem, ask for support")
                return
            print("✅ ssh setup done")
        self.output.clear_output()
        self.subtitle.value = "<h3>Setup Codes and Uenvs. It will take several minutes</h3>"
        with self.output:
            # setup codes and uenvs
            uenvs = setup_codes(['cp2k.yml', 'stm.yml', 'overlap.yml', 'pw.yml', 'pp.yml', 'projwfc.yml', 'dos.yml'])
            qe_uenv = next((env for env in uenvs if 'espresso' in env), None)
            if len(uenvs) >0:
                uenvs_ok = manage_uenv_images(remotehost, uenvs)
                if not uenvs_ok:
                    print("❌ uenvs not set up correctly ask for help")
                    return
                
        if self.qe_postprocess_checkbox.value:
            self.output.clear_output()
            self.subtitle.value = "<h3>Setup Phonopy and Critic2</h3>"
            with self.output:
                phonopy_ok = setup_phonopy(cscs_username)
                if not phonopy_ok:
                    print("❌ phonopy not set up correctly ask for help")
                    return
                print("✅ phonopy setup done")
            self.output.clear_output()
            self.subtitle.value = "<h3>Setup Critic2</h3>"  
            with self.output:
                print("Creating conda environment will take a while")
                critic2_ok = setup_critic2(cscs_username,qe_uenv)
                if not critic2_ok:
                    print("❌ critic2 not set up correctly ask for help")
                    return
                print("✅ critic2 setup done")
        return

# Example function
def get_start_widget(appbase, jupbase, notebase):
    return ConfigAiiDAlabApp()  # ✅ Return instance of the class

