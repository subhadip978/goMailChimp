package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"text/template"
	"time"

	"github.com/subhadipdas/gomailer/internal/domain"
	"github.com/subhadipdas/gomailer/internal/repository"
	"golang.org/x/time/rate"
)

// Job Payload sent through the channel
type EmailJob struct {
	CampaignJob domain.CampaignJob
	Template    domain.Template
	Contact     domain.Contact
	Retries     int
}

type WorkerPool struct {
	jobQueue     chan EmailJob      // Email queue for pending jobs
	deadLetter   chan EmailJob      // Dead-letter queue for failed jobs
	workerCount  int
	repo         repository.CampaignRepository
	rateLimiter  *rate.Limiter      // Rate limiting
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewWorkerPool(workerCount int, repo repository.CampaignRepository) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		// Buffered channel: Decouples producer from consumer, allowing burst of jobs
		jobQueue:    make(chan EmailJob, 1000), 
		deadLetter:  make(chan EmailJob, 100),
		workerCount: workerCount,
		repo:        repo,
		// Rate limiting: 10 emails per second
		rateLimiter: rate.NewLimiter(10, 1),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (w *WorkerPool) Start() {
	log.Printf("Starting worker pool with %d workers\n", w.workerCount)

	// Concurrency Pattern: Worker Pool
	// We spin up a fixed number of goroutines (workers) that all read from the same jobQueue channel.
	// This ensures we don't overwhelm the system with too many concurrent routines, 
	// while still processing jobs in parallel.
	for i := 1; i <= w.workerCount; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	// Concurrency Pattern: Dedicated Goroutine for Dead-Letter Queue
	// We process failed jobs separately so it doesn't block main email sending.
	w.wg.Add(1)
	go w.processDeadLetters()

	// Concurrency Pattern: Periodic Job Poller
	// Periodically fetch pending jobs from the database and feed them to the jobQueue.
	w.wg.Add(1)
	go w.pollJobs()
}

func (w *WorkerPool) Stop() {
	log.Println("Stopping worker pool...")
	w.cancel() // Signals all context-aware goroutines to stop
	w.wg.Wait() // Waits for all goroutines to finish processing
	log.Println("Worker pool stopped cleanly")
}

func (w *WorkerPool) worker(id int) {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			log.Printf("[Worker %d] Shutting down\n", id)
			return
		case job := <-w.jobQueue:
			// Concurrency Pattern: Rate Limiting
			// Block until the rate limiter allows the next event
			if err := w.rateLimiter.Wait(w.ctx); err != nil {
				continue
			}

			w.processJob(id, job)
		}
	}
}

func (w *WorkerPool) processJob(workerID int, job EmailJob) {
	log.Printf("[Worker %d] Processing Job ID %d for %s\n", workerID, job.CampaignJob.ID, job.Contact.Email)

	// 1. Template rendering per recipient
	renderedBody, err := w.renderTemplate(job.Template.HTMLBody, job.Contact)
	if err != nil {
		w.handleJobFailure(job, fmt.Sprintf("Template rendering failed: %v", err))
		return
	}

	// 2. Mock Email Sending
	err = w.mockSendEmail(job.Contact.Email, job.Template.Subject, renderedBody)
	
	if err != nil {
		// 3. Retries logic
		w.handleJobFailure(job, err.Error())
	} else {
		// Success
		_ = w.repo.UpdateJobStatus(job.CampaignJob.ID, domain.JobStatusSent, "")
		log.Printf("[Worker %d] Sent email to %s\n", workerID, job.Contact.Email)
	}
}

func (w *WorkerPool) handleJobFailure(job EmailJob, errMsg string) {
	if job.Retries < 3 {
		job.Retries++
		log.Printf("Job %d failed (%s). Retrying (%d/3)...\n", job.CampaignJob.ID, errMsg, job.Retries)
		// Wait a bit before retry (Exponential backoff could be used here)
		time.Sleep(time.Second * time.Duration(job.Retries))
		w.jobQueue <- job // Re-queue
	} else {
		log.Printf("Job %d completely failed after 3 retries. Moving to Dead-Letter Queue.\n", job.CampaignJob.ID)
		w.deadLetter <- job
		_ = w.repo.UpdateJobStatus(job.CampaignJob.ID, domain.JobStatusFailed, errMsg)
	}
}

func (w *WorkerPool) processDeadLetters() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case job := <-w.deadLetter:
			// In a real system, you might save this to a specific "failed_jobs" table
			// or alert an admin.
			log.Printf("[DeadLetter] Job ID %d for %s failed permanently.\n", job.CampaignJob.ID, job.Contact.Email)
		}
	}
}

func (w *WorkerPool) pollJobs() {
	defer w.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			// Fetch pending jobs
			jobs, err := w.repo.GetPendingJobs(100)
			if err != nil {
				log.Printf("Error polling jobs: %v\n", err)
				continue
			}

			for _, j := range jobs {
				if j.Contact == nil || j.Campaign == nil || j.Campaign.Template == nil {
					continue
				}
				
				// Mark as processing so it's not picked up by next poll
				w.repo.UpdateJobStatus(j.ID, domain.JobStatusProcessing, "")

				// Send to queue
				w.jobQueue <- EmailJob{
					CampaignJob: j,
					Template:    *j.Campaign.Template,
					Contact:     *j.Contact,
					Retries:     0,
				}
			}
		}
	}
}

func (w *WorkerPool) renderTemplate(tmplBody string, contact domain.Contact) (string, error) {
	t, err := template.New("email").Parse(tmplBody)
	if err != nil {
		return "", err
	}
	
	data := map[string]interface{}{
		"name":             contact.Name,
		"company":          contact.Company,
		"email":            contact.Email,
		"unsubscribe_link": "http://localhost:8080/unsubscribe?email=" + contact.Email,
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (w *WorkerPool) mockSendEmail(to, subject, body string) error {
	// Simulate network delay
	time.Sleep(100 * time.Millisecond)
	// Random failure simulation could be added here
	return nil
}
