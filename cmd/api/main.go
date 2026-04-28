package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/subhadipdas/gomailer/internal/config"
	"github.com/subhadipdas/gomailer/internal/db"
	"github.com/subhadipdas/gomailer/internal/handler"
	"github.com/subhadipdas/gomailer/internal/repository"
	"github.com/subhadipdas/gomailer/internal/service"
	"github.com/subhadipdas/gomailer/internal/worker"
)

func main() {
	// 1. Load Configuration
	cfg := config.LoadConfig()

	// 2. Connect to Database & Auto-migrate
	database, err := db.ConnectDB(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// 3. Initialize Repositories
	tmplRepo := repository.NewTemplateRepository(database)
	contactRepo := repository.NewContactRepository(database)
	campaignRepo := repository.NewCampaignRepository(database)

	// 4. Initialize Services
	tmplSvc := service.NewTemplateService(tmplRepo)
	contactSvc := service.NewContactService(contactRepo)
	campaignSvc := service.NewCampaignService(campaignRepo, contactRepo)

	// 5. Initialize & Start Worker Pool
	// We use 5 workers as requested in the original script
	workerPool := worker.NewWorkerPool(5, campaignRepo)
	workerPool.Start()
	defer workerPool.Stop()

	// 6. Initialize Handlers
	tmplHandler := handler.NewTemplateHandler(tmplSvc)
	contactHandler := handler.NewContactHandler(contactSvc)
	campaignHandler := handler.NewCampaignHandler(campaignSvc)

	// 7. Setup Gin Router
	r := gin.Default()

	// API Routes
	api := r.Group("/api/v1")
	{
		// Templates
		api.POST("/templates", tmplHandler.CreateTemplate)
		api.GET("/templates/:id", tmplHandler.GetTemplate)
		api.POST("/templates/:id/preview", tmplHandler.PreviewTemplate)

		// Contacts & Lists
		api.POST("/lists", contactHandler.CreateList)
		api.POST("/contacts/import-csv", contactHandler.ImportCSV)

		// Campaigns
		api.POST("/campaigns", campaignHandler.CreateCampaign)
		api.POST("/campaigns/:id/start", campaignHandler.StartCampaign)
	}

	// 8. Start Server
	log.Printf("Server starting on port %s\n", cfg.Port)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
