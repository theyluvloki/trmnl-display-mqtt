package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Version information
var (
	version   = "0.2.1-mqtt"
	commit    = "unknown"
	buildDate = "unknown"
)

// TerminalResponse represents the JSON structure expected from your MQTT message payload
type TerminalResponse struct {
	ImageURL    string `json:"url"`
	Filename    string `json:"filename"`
	RefreshRate int    `json:"refresh_rate"`
}

// Config holds application configuration including MQTT broker details and credentials
type Config struct {
	BrokerURL string `json:"broker_url,omitempty"` // e.g., "tcp://192.168.1.50:1883"
	Topic     string `json:"topic,omitempty"`      // e.g., "trmnl/display"
	Username  string `json:"username,omitempty"`   // MQTT Username
	Password  string `json:"password,omitempty"`   // MQTT Password
	DeviceID  string `json:"device_id,omitempty"`
}

// DisplayConfig maps the adapter setting from show_img.json
type DisplayConfig struct {
	Adapter string `json:"adapter"`
}

// AppOptions holds command line options
type AppOptions struct {
	DarkMode bool
	Verbose  bool
}
func main() {
	options := parseCommandLineArgs()
	setupSignalHandling()

	// Completely hide the console block cursor while the app is running
	if f, err := os.OpenFile("/dev/tty1", os.O_WRONLY, 0644); err == nil {
		f.WriteString("\033[?25l")
		f.Close()
	}
	_ = exec.Command("setterm", "--cursor", "off").Run()

	// Ensure the cursor restores automatically if the app shuts down
	defer func() {
		if f, err := os.OpenFile("/dev/tty1", os.O_WRONLY, 0644); err == nil {
			f.WriteString("\033[?25h")
			f.Close()
		}
		_ = exec.Command("setterm", "--cursor", "on").Run()
	}()

	if options.Verbose {
		fmt.Println("Starting TRMNL MQTT client...")
		if options.DarkMode {
			fmt.Println("Dark mode enabled - images will be inverted")
		}
	}

        // XDG Config Directory setup
	configHome := os.Getenv("XDG_CONFIG_HOME")
	if configHome == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			fmt.Printf("Error getting home directory: %v\n", err)
			os.Exit(1)
		}
		configHome = filepath.Join(homeDir, ".config")
	}
	configDir := filepath.Join(configHome, "trmnl")
	err := os.MkdirAll(configDir, 0755)
	if err != nil {
		fmt.Printf("Error creating config directory: %v\n", err)
		os.Exit(1)
	}

	// Load configuration
	config := loadConfig(configDir)

	// Set defaults if missing and prompt user if broker is unconfigured
	if config.BrokerURL == "" {
		reader := bufio.NewReader(os.Stdin)

		fmt.Println("MQTT Broker URL not found.")
		fmt.Print("Enter broker URL (e.g., tcp://192.168.1.50:1883): ")
		config.BrokerURL, _ = reader.ReadString('\n')
		config.BrokerURL = strings.TrimSpace(config.BrokerURL)

		fmt.Print("Enter MQTT Username (leave blank if none): ")
		config.Username, _ = reader.ReadString('\n')
		config.Username = strings.TrimSpace(config.Username)

		fmt.Print("Enter MQTT Password (leave blank if none): ")
		config.Password, _ = reader.ReadString('\n')
		config.Password = strings.TrimSpace(config.Password)

		saveConfig(configDir, config)
	}
	if config.Topic == "" {
		config.Topic = "trmnl/display"
		saveConfig(configDir, config)
	}

	if options.Verbose {
		fmt.Printf("Connecting to MQTT Broker: %s\n", config.BrokerURL)
		fmt.Printf("Listening on Topic: %s\n", config.Topic)
	}

	// Create a temporary directory for storing images
	tmpDir, err := os.MkdirTemp("", "trmnl-display")
	if err != nil {
		fmt.Printf("Error creating temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	frames := 0

	// Configure MQTT Client Options
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetClientID(fmt.Sprintf("trmnl-display-%d", time.Now().UnixNano()))
	opts.SetOrderMatters(false)

	// Set credentials if provided
	if config.Username != "" {
		opts.SetUsername(config.Username)
	}
	if config.Password != "" {
		opts.SetPassword(config.Password)
	}

	// Define message handler callback when an MQTT update comes in
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		if options.Verbose {
			fmt.Printf("Received message on topic [%s]\n", msg.Topic())
		}

		var terminal TerminalResponse
		if err := json.Unmarshal(msg.Payload(), &terminal); err != nil {
			fmt.Printf("Error parsing MQTT JSON payload: %v\n", err)
			return
		}

		filename := terminal.Filename
		if filename == "" {
			filename = "display.png"
		}
		filePath := filepath.Join(tmpDir, filename)

		// Download the image specified in the payload
		if err := downloadImage(terminal.ImageURL, filePath); err != nil {
			fmt.Printf("Error downloading image: %v\n", err)
			return
		}

		// Render image to screen
		if err := displayImage(filePath, options, frames); err != nil {
			fmt.Printf("Error displaying image: %v\n", err)
			return
		}

		frames++
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Failed to connect to MQTT broker: %v\n", token.Error())
		os.Exit(1)
	}

	// Subscribe to the designated topic
	if token := client.Subscribe(config.Topic, 0, nil); token.Wait() && token.Error() != nil {
		fmt.Printf("Failed to subscribe to topic: %v\n", token.Error())
		os.Exit(1)
	}

	fmt.Println("Connected and waiting for MQTT messages. Press Ctrl+C to exit.")

	// Block forever to keep the background MQTT listener running
	select {}
}

// downloadImage handles pulling the image from the URL provided via MQTT
func downloadImage(url, destPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

// setupSignalHandling handles graceful exits on interruption
func setupSignalHandling() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-c
		fmt.Println("\nReceived termination signal. Cleaning up...")
		os.Exit(0)
	}()
}

// parseCommandLineArgs parses command line switches
func parseCommandLineArgs() AppOptions {
	darkMode := flag.Bool("d", false, "Enable dark mode (invert image pixels)")
	showVersion := flag.Bool("v", false, "Show version information")
	verbose := flag.Bool("verbose", true, "Enable verbose output")
	quiet := flag.Bool("q", false, "Quiet mode (disable verbose output)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("trmnl-display mqtt version %s (commit: %s, built: %s)\n", version, commit, buildDate)
		os.Exit(0)
	}

	return AppOptions{
		DarkMode: *darkMode,
		Verbose:  *verbose && !*quiet,
	}
}

// Helper to locate show_img.json safely across user or root contexts
func getShowImgConfigPath() string {
	homeDir, err := os.UserHomeDir()
	if err == nil {
		p := filepath.Join(homeDir, ".config", "trmnl", "show_img.json")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if _, err := os.Stat("/home/dietpi/.config/trmnl/show_img.json"); err == nil {
		return "/home/dietpi/.config/trmnl/show_img.json"
	}
	return "/root/.config/trmnl/show_img.json"
}

// displayImage conditionally renders using ffmpeg (for HDMI framebuffer) or show_img (for e-paper)
func displayImage(imagePath string, options AppOptions, frames int) error {
        adapter := "framebuffer" // default fallback

        // Read adapter choice from config generated by install script
        configPath := getShowImgConfigPath()
        if data, err := os.ReadFile(configPath); err == nil {
                var dispCfg DisplayConfig
                if json.Unmarshal(data, &dispCfg) == nil && dispCfg.Adapter != "" {
                        adapter = dispCfg.Adapter
                }
        }

        renderMethod := ""

        if adapter == "framebuffer" {
                // Use ffmpeg with explicit 900x1600 scaling and pixel format for fbdev
                args := []string{
                        "-y",
                        "-i", imagePath,
                        "-vf", "scale=1600:900:force_original_aspect_ratio=decrease,pad=1600:900:(ow-iw)/2:(oh-ih)/2",
                        "-pix_fmt", "rgb565le",
                        "-f", "fbdev",
                        "/dev/fb0",
                }

                cmd := exec.Command("ffmpeg", args...)
                var stderr strings.Builder
                cmd.Stderr = &stderr

                if err := cmd.Run(); err != nil {
                        fmt.Printf("WARNING: ffmpeg failed (%v). FFmpeg error: %s\nFalling back to fbi...\n", err, stderr.String())

                        // Fallback to fbi if ffmpeg fails
                        fallbackArgs := []string{
                                "-d", "/dev/fb0",
                                "-T", "1",
                                "-noverbose",
                                "-once",
                                "-a",
                                imagePath,
                        }
                        if fbErr := exec.Command("fbi", fallbackArgs...).Run(); fbErr != nil {
                                return fmt.Errorf("framebuffer rendering failed: %v (ffmpeg error: %v)", fbErr, err)
                        }
                        renderMethod = "framebuffer (fbi fallback)"
                } else {
                        renderMethod = "framebuffer (ffmpeg)"
                }
        } else {
                // Use original show_img binary for e-paper displays, retaining the refresh logic
                var sb strings.Builder
                var sb2 strings.Builder
                var sb3 strings.Builder

                sb.WriteString("file=")
                sb.WriteString(imagePath)

                sb2.WriteString("invert=")
                if options.DarkMode {
                        sb2.WriteString("true")
                } else {
                        sb2.WriteString("false")
                }

                sb3.WriteString("mode=")
                if (frames & 3) == 0 { // Full refresh every 4 updates to clear e-paper ghosting
                        sb3.WriteString("fast")
                } else {
                        sb3.WriteString("partial") // Smooth refresh
                }

                err := exec.Command("show_img", sb.String(), sb2.String(), sb3.String()).Run()
                if err != nil {
                        return fmt.Errorf("show_img tool missing or failed; error = %v", err)
                }
                renderMethod = "show_img (e-paper)"
        }

        if options.Verbose {
                fmt.Printf("Displayed via [%s]: %s (Frame %d)\n", renderMethod, imagePath, frames)
        }
        return nil
}

func loadConfig(configDir string) Config {
	configFile := filepath.Join(configDir, "config.json")
	config := Config{}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return config
	}

	_ = json.Unmarshal(data, &config)
	return config
}

func saveConfig(configDir string, config Config) {
	configFile := filepath.Join(configDir, "config.json")
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Printf("Error saving config: %v\n", err)
		return
	}

	_ = os.WriteFile(configFile, data, 0600)
}
